#!/bin/zsh
# Builds the monet-demo iOS app and runs it on a simulator.
#
#   ./build.sh                 # build + install + launch on the first booted device
#   ./build.sh <UDID>          # ... on a specific device (boots it if needed)
#   ./build.sh --build-only    # just assemble the .app bundle
#
# The app links the Rust static library (monet-demo, which embeds the
# Swift shim + trampoline from monet-urlsession) and calls one C function;
# everything else — the battery, the transports, the loopback servers —
# is the same code the host tests run.

set -euo pipefail

ROOT=${0:A:h}/../..
APP_DIR=${ROOT}/platform/ios-demo
BUNDLE=${APP_DIR}/build/MonetDemo.app
TARGET=aarch64-apple-ios-sim
RUST_LIB=${ROOT}/target/${TARGET}/debug/libmonet_demo.a

# ---- 1. Rust static library (embeds the Swift shim + trampoline) ----
print "== building libmonet_demo.a (${TARGET})"
(cd ${ROOT} && cargo build -p monet-demo --target ${TARGET})

# ---- 2. Assemble the .app bundle ----
rm -rf ${APP_DIR}/build
mkdir -p ${BUNDLE}
cp ${APP_DIR}/app/Info.plist ${BUNDLE}/Info.plist

print "== compiling the app (iphonesimulator SDK)"
xcrun --sdk iphonesimulator swiftc \
    -target arm64-apple-ios18.0-simulator \
    -parse-as-library -emit-executable \
    -I ${APP_DIR}/include \
    ${APP_DIR}/app/DemoApp.swift \
    -L ${ROOT}/target/${TARGET}/debug -lmonet_demo \
    -framework Foundation -framework UIKit -framework SwiftUI \
    -lc++ \
    -o ${BUNDLE}/MonetDemo

codesign --force --sign - ${BUNDLE}
print "== bundle ready: ${BUNDLE}"

if [[ ${1:-} == "--build-only" ]]; then
    exit 0
fi

# ---- 3. Install + launch on a simulator ----
UDID=${1:-$(xcrun simctl list devices booted | grep -oE '[0-9A-F-]{36}' | head -1)}
if [[ -z ${UDID} ]]; then
    UDID=$(xcrun simctl list devices available | grep -oE '[0-9A-F-]{36}' | head -1)
    print "== booting ${UDID}"
    xcrun simctl boot ${UDID}
fi

print "== installing on ${UDID}"
xcrun simctl install ${UDID} ${BUNDLE}
print "== launching (console attached)"
exec xcrun simctl launch --console-pty ${UDID} com.monet.demo
