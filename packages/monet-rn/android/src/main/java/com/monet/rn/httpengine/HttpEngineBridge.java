// The mechanical bridge for the platform HttpEngine transport: forwards
// Cronet's Java callbacks into the Rust crate with the request id. All
// state lives on the Rust side; the native methods are bound onto these
// declarations at bootstrap (RegisterNatives in the Rust init — the
// class name is a parameter, which is why this copy lives here under
// the monet-rn package while the demo app keeps its own).
package com.monet.rn.httpengine;

import android.net.http.HttpException;
import android.net.http.UrlRequest;
import android.net.http.UrlResponseInfo;
import java.nio.ByteBuffer;

public final class HttpEngineBridge {
  static {
    // The Rust aggregate library is linked statically into the
    // rust-turbo JNI glue library.
    System.loadLibrary("rustturbo");
  }

  private HttpEngineBridge() {}

  public static native void onStarted(
      long id, UrlRequest request, UrlResponseInfo info);

  public static native void onReadCompleted(
      long id, UrlRequest request, UrlResponseInfo info, ByteBuffer byteBuffer);

  public static native void onRedirect(
      long id, UrlRequest request, UrlResponseInfo info, String location);

  public static native void onSucceeded(
      long id, UrlRequest request, UrlResponseInfo info);

  public static native void onFailed(
      long id, UrlRequest request, UrlResponseInfo info, HttpException error);

  public static native void onCanceled(
      long id, UrlRequest request, UrlResponseInfo info);

  // The chunked upload provider's reach (v2/W2d): fills `buffer`'s
  // [position, position + remaining) window from the Rust feeder's
  // queue — > 0 bytes written, 0 the end, -1 nothing yet (the read
  // parks), -2 the stream failed.
  public static native int takeUploadChunk(
      long id, ByteBuffer buffer, int position, int remaining);

  // The provider's close: ends the Rust-side upload state.
  public static native void closeUpload(long id);
}
