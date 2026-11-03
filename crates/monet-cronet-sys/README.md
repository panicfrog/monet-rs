# monet-cronet-sys

Raw FFI declarations for the Cronet C API.

## Vendored headers

`include/` is vendored verbatim from the official Chromium 143.7445.0
release package:

```
https://commondatastorage.googleapis.com/chromium-cronet/android/143.0.7445.0/Release/cronet/include/
```

- `cronet.idl_c.h` — the full generated C API (from
  `components/cronet/native/cronet.idl`)
- `cronet_c.h` — umbrella header (adds the stream-engine + mock-cert-verifier
  extras)
- `cronet_export.h` — export macros
- `bidirectional_stream_c.h` — gRPC bidirectional streams (vendored for
  completeness; not bound)
- `bindgen_wrapper.h` — our addition: pulls in `<stdbool.h>` because the
  generated header uses C99 `bool` without including it (native consumers
  are C++)

**Version ceiling**: upstream removed the C API from Chromium 146 onward —
146+ release packages ship no `include/`. The header is byte-identical
143→145, so the practical upgrade ceiling is
[145.0.7569.0](https://commondatastorage.googleapis.com/chromium-cronet/android/145.0.7569.0/Release/cronet/include/).
Beyond that the options are the SagerNet fork (which still carries the C
API) or moving to the Java API over JNI.

## Regenerating `src/bindings.rs`

`bindings.rs` is checked in so consumers need neither libclang nor bindgen.
To regenerate after a header change:

```sh
cargo install bindgen-cli
bindgen crates/monet-cronet-sys/include/bindgen_wrapper.h \
  --allowlist-function 'Cronet_.*' \
  --allowlist-type 'Cronet_.*' \
  --allowlist-var 'Cronet_.*' \
  --no-layout-tests \
  -o crates/monet-cronet-sys/src/bindings.rs
```

## Linking

Off by default. Enable the `link` feature to emit link directives for the
real library:

- `CRONET_LIB_DIR` set → `rustc-link-search=native=$CRONET_LIB_DIR` +
  `rustc-link-lib=dylib=cronet` (any host with a matching libcronet build)
- Android target without `CRONET_LIB_DIR` → `rustc-link-lib=dylib=cronet`
  only; the RN app bundles `libcronet.<version>.so` through the
  `org.chromium.net:cronet-embedded` AAR and gradle performs the final link
- Anything else → build error (fail loudly rather than silently link nothing)

Without `link`, nothing is linked at all — that is what host tests rely on:
the `monet-cronet-fake` dev-dependency provides a C fake implementing the
same symbols.
