//! Link directives for libcronet. Header bindings themselves are checked in
//! (`src/bindings.rs`); regenerate with bindgen-cli, see README.md.

use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=CRONET_LIB_DIR");
    println!("cargo:rerun-if-env-changed=CRONET_LIB_NAME");

    if env::var_os("CARGO_FEATURE_LINK").is_none() {
        // No link: host test builds link a fake; embedders opt in with the
        // `link` feature.
        return;
    }

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let name = env::var("CRONET_LIB_NAME").unwrap_or_else(|_| "cronet".to_string());

    if let Some(dir) = env::var_os("CRONET_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", dir.to_string_lossy());
        println!("cargo:rustc-link-lib=dylib={name}");
    } else if target_os == "android" {
        // The RN app bundles libcronet.<version>.so through the
        // cronet-embedded AAR; the final link happens in gradle, so a search
        // path is neither available nor needed here.
        println!("cargo:rustc-link-lib=dylib={name}");
    } else {
        panic!(
            "monet-cronet-sys: the `link` feature requires CRONET_LIB_DIR \
             pointing at the directory holding libcronet.{name}.so \
             (non-Android host build)"
        );
    }
}
