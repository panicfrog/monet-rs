// The HttpEngine bootstrap entry: hands the JVM and the app Context to
// the Rust side, which stores them and registers the native callbacks
// onto HttpEngineBridge (RegisterNatives). Idempotent; must run before
// the first request on the `httpengine` stack — MonetRnPackage calls it
// automatically during app startup (and an app may call it itself, e.g.
// from MainActivity, to make the ordering explicit).
//
// The native side is a no-op when the crate was built without the
// `httpengine` feature, so this class is safe to reference either way.
package com.monet.rn.httpengine;

import android.content.Context;

public final class MonetBootstrap {
  static {
    // The native implementation lives in the Rust aggregate library,
    // linked statically into the rust-turbo JNI glue library.
    System.loadLibrary("rustturbo");
  }

  private MonetBootstrap() {}

  public static void init(Context context) {
    try {
      nativeInit(context);
    } catch (Throwable t) {
      // UnsatisfiedLinkError only when something is misassembled (the
      // symbol is compiled in every configuration); surfacing it as a
      // crash at startup is worse than running on the fallback stack,
      // which is exactly what happens when the Rust init never ran.
      android.util.Log.w("MonetBootstrap", "init failed; the rustls fallback applies", t);
    }
  }

  private static native void nativeInit(Context context);
}
