//! Build orchestration: cxx bridge header → modulemap → Swift static
//! library (with generated clang header) → C++ trampoline → link.
//!
//! The build order resolves the mutual dependency between Swift and cxx:
//! the Swift shim imports the cxx-generated header (module
//! `monet_urlsession_bridge`), while the trampoline includes the clang
//! header that compiling the Swift shim produces.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const MODULE_NAME: &str = "MonetUrlsessionBridge";

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if !matches!(target_os.as_str(), "macos" | "ios") {
        panic!("monet-urlsession requires macOS or iOS targets (found {target_os})");
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    // 1. cxx bridge: generates headers + cargo directives.
    let cxx_out = out_dir.join("cxxbridge");
    cxx_build::bridge("src/ffi.rs")
        .flag_if_supported("-std=c++17")
        .compile("monet-urlsession-cxx");
    let cxx_include = cxx_out.join("include");

    // The generated bridge header, e.g. <out>/cxxbridge/include/monet-urlsession/src/ffi.rs.h
    let bridge_header =
        find_bridge_header(&cxx_include).expect("cxx_build did not generate a bridge header");
    let bridge_header_rel = bridge_header
        .strip_prefix(&cxx_include)
        .expect("header inside include dir");

    // 2. Wrap it in a modulemap so Swift can import it. The pointer-based
    // wrappers below are the Swift shim's data-plane entry points for
    // slice parameters: Swift never constructs rust::Slice (a C++ class
    // template) itself. Signatures mirror the extern "Rust" functions in
    // src/ffi.rs; pure-primitive events are called directly.
    let module_dir = out_dir.join("bridge-module");
    std::fs::create_dir_all(&module_dir).unwrap();
    std::fs::write(
        module_dir.join("rust_bridge.h"),
        format!(
            "#include \"{rel}\"\n#include \"rust/cxx.h\"\n\n\
             inline void monet_on_header(\
             ::std::uint64_t id, const ::std::uint8_t *name, ::std::size_t name_len, \
             const ::std::uint8_t *value, ::std::size_t value_len) noexcept {{\n\
             \x20 on_header(id, rust::Slice<const ::std::uint8_t>(name, name_len), \n\
             \x20                       rust::Slice<const ::std::uint8_t>(value, value_len));\n\
             }}\n\
             inline void monet_on_chunk(\
             ::std::uint64_t id, const ::std::uint8_t *data, ::std::size_t len) noexcept {{\n\
             \x20 on_chunk(id, rust::Slice<const ::std::uint8_t>(data, len));\n\
             }}\n\
             inline void monet_on_redirect(\
             ::std::uint64_t id, const ::std::uint8_t *location, ::std::size_t len) noexcept {{\n\
             \x20 on_redirect(id, rust::Slice<const ::std::uint8_t>(location, len));\n\
             }}\n\
             inline void monet_on_error(\
             ::std::uint64_t id, ::std::int32_t code, const ::std::uint8_t *msg, ::std::size_t len) noexcept {{\n\
             \x20 on_error(id, code, rust::Slice<const ::std::uint8_t>(msg, len));\n\
             }}\n",
            rel = bridge_header_rel.display()
        ),
    )
    .unwrap();
    std::fs::write(
        module_dir.join("module.modulemap"),
        "module monet_urlsession_bridge {\n    header \"rust_bridge.h\"\n    export *\n}\n",
    )
    .unwrap();

    // 3. Compile the Swift shim into a static library + clang header.
    let swift_out = out_dir.join("swift");
    std::fs::create_dir_all(&swift_out).unwrap();
    let swift_lib = swift_out.join(format!("lib{MODULE_NAME}.a"));
    let swift_header = swift_out.join(format!("{MODULE_NAME}-Swift.h"));

    let swift_files = collect_swift_sources(&manifest_dir.join("swift"));
    assert!(!swift_files.is_empty(), "no Swift sources found in swift/");

    let mut swiftc = swiftc_command(&target_os);
    swiftc
        .arg("-parse-as-library")
        .arg("-emit-library")
        .arg("-static")
        .arg("-cxx-interoperability-mode=default")
        .arg("-module-name")
        .arg(MODULE_NAME)
        .arg("-I")
        .arg(&module_dir)
        .arg("-I")
        .arg(&cxx_include)
        .arg("-I")
        .arg(cxx_out.join("crate"))
        .arg("-emit-clang-header-path")
        .arg(&swift_header)
        .arg("-o")
        .arg(&swift_lib)
        .args(&swift_files);
    run(swiftc, "swiftc (shim)");

    // 4. Compile the trampoline, which includes the Swift-generated header.
    let mut trampoline = cc::Build::new();
    trampoline
        .cpp(true)
        .std("c++17")
        .include(&swift_out)
        .include(&cxx_include)
        .file(manifest_dir.join("cpp/trampoline.cpp"))
        .compile("monet-urlsession-trampoline");

    // 5. Link the Swift static library and its runtime dependencies.
    println!("cargo:rustc-link-search=native={}", swift_out.display());
    println!("cargo:rustc-link-lib=static={MODULE_NAME}");
    println!("cargo:rustc-link-lib=framework=Foundation");
    // Swift runtime (the static archive leaves swiftCore symbols undefined).
    println!("cargo:rustc-link-search=native=/usr/lib/swift");
    println!("cargo:rustc-link-lib=dylib=swiftCore");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=shim.h");
    println!("cargo:rerun-if-changed=src/ffi.rs");
    println!("cargo:rerun-if-changed=cpp/trampoline.cpp");
    for file in &swift_files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
}

/// The Swift sources of the shim, in a stable (sorted) order.
fn collect_swift_sources(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "swift"))
        .collect();
    files.sort();
    files
}

/// Resolves `swiftc` (host build) or the SDK-wrapped cross invocation.
fn swiftc_command(target_os: &str) -> Command {
    let mut command = Command::new("xcrun");
    match target_os {
        "macos" => {
            command.arg("swiftc");
        }
        "ios" => {
            let sdk = String::from_utf8(
                Command::new("xcrun")
                    .args(["--sdk", "iphoneos", "--show-sdk-path"])
                    .output()
                    .expect("xcrun iphoneos sdk")
                    .stdout,
            )
            .expect("utf8 sdk path")
            .trim()
            .to_string();
            command.args(["--sdk", "iphoneos", "swiftc"]);
            command.arg("-target").arg("arm64-apple-ios17.0");
            command.arg("-sdk").arg(sdk);
        }
        _ => unreachable!(),
    }
    command
}

fn find_bridge_header(include_dir: &Path) -> Option<PathBuf> {
    // The bridge module's header lands at <include>/<pkg>/src/ffi.rs.h;
    // skip cxx's own rust/cxx.h which lives in the same tree.
    fn walk(dir: &Path, found: &mut Option<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, found);
                } else if path.file_name().is_some_and(|n| n == "ffi.rs.h") {
                    found.get_or_insert(path);
                }
            }
        }
    }
    let mut found = None;
    walk(include_dir, &mut found);
    found
}

fn run(mut command: Command, what: &str) {
    let status = command
        .status()
        .unwrap_or_else(|e| panic!("{what} failed to start: {e}"));
    if !status.success() {
        panic!("{what} failed with {status}");
    }
}
