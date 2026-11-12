//! The vendor freshness guard (v2/W6).
//!
//! The app's aggregate build compiles the vendored copies in vendor/, not
//! the repo's crates/ — and three separate sessions lost hours to a stale
//! vendor silently compiling (the M5 hooks, W2c, and W3a builds all bit
//! this: the binary shipped days-old Rust while the source looked
//! current). This guard makes staleness a build failure instead.
//!
//! The check only runs when the repo's crates/ is reachable — a dev build
//! through node_modules' symlink back into the repo. A published package
//! has no source of truth beside itself and skips the check silently.
//!
//! Cargo.toml is exempt: sync-vendor rewrites the vendored manifests
//! (workspace-inheritance mirroring), so they are intentionally different
//! from crates/' originals. Everything else — src/, swift/, cpp/, shim.h,
//! tests/ — must be byte-identical.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The vendored crates (the sync script's list).
const CRATES: &[&str] = &[
    "monet-core",
    "monet-tls",
    "monet-rustls",
    "monet-ws",
    "monet-urlsession",
    "monet-httpengine",
];

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let repo_crates = manifest.join("../../crates");
    if !repo_crates.is_dir() {
        // A published package (or a consumer without the repo) — nothing
        // to compare against.
        return;
    }
    // The guard must rerun when either side moves: without these,
    // cargo caches the script's success and a crates/ edit alone (the
    // exact stale-vendor scenario) never re-triggers it.
    for name in CRATES {
        println!("cargo:rerun-if-changed={}", repo_crates.join(name).display());
        println!(
            "cargo:rerun-if-changed={}",
            manifest.join("vendor").join(name).display()
        );
    }
    for name in CRATES {
        let source = repo_crates.join(name);
        let vendored = manifest.join("vendor").join(name);
        if !source.is_dir() || !vendored.is_dir() {
            continue;
        }
        let source_files = tree(&source);
        let vendored_files = tree(&vendored);
        if source_files != vendored_files {
            let first = source_files
                .iter()
                .chain(vendored_files.iter())
                .filter(|(rel, _)| source_files.get(*rel) != vendored_files.get(*rel))
                .map(|(rel, _)| rel.clone())
                .next()
                .unwrap_or_default();
            panic!(
                "monet-rn: vendor/{name} is stale against crates/{name} \
                 (first difference: {first}) — run the sync before building: \
                 cd packages/monet-rn && npm run sync-vendor"
            );
        }
    }
}

/// The tree's file map: relative path → content hash. Cargo.toml is
/// skipped (the sync script rewrites the vendored manifests).
fn tree(root: &Path) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, u64>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(root, &path, out);
            continue;
        }
        let Some(rel) = path.strip_prefix(root).ok().and_then(|p| p.to_str()) else {
            continue;
        };
        if rel == "Cargo.toml" || rel.ends_with("/Cargo.toml") {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        out.insert(rel.to_string(), fxhash(&bytes));
    }
}

/// A cheap stable content hash (FNV-1a 64) — collision odds are
/// irrelevant here (the comparison is against near-identical trees).
fn fxhash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
