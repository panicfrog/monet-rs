// Forwards every Cronet callback to HttpEngineBridge with the request id.
// Java, not Kotlin, on purpose: no kotlin-gradle-plugin dependency for
// the consumer's build — the bridge is pure interface forwarding.
package com.monet.rn.httpengine;

import android.net.http.HttpException;
import android.net.http.UrlRequest;
import android.net.http.UrlResponseInfo;
import java.nio.ByteBuffer;

public final class MonetRequestCallback implements UrlRequest.Callback {
  private final long id;

  public MonetRequestCallback(long id) {
    this.id = id;
  }

  @Override
  public void onResponseStarted(UrlRequest request, UrlResponseInfo info) {
    HttpEngineBridge.onStarted(id, request, info);
  }

  @Override
  public void onReadCompleted(
      UrlRequest request, UrlResponseInfo info, ByteBuffer byteBuffer) {
    HttpEngineBridge.onReadCompleted(id, request, info, byteBuffer);
  }

  @Override
  public void onRedirectReceived(
      UrlRequest request, UrlResponseInfo info, String newLocationUrl) {
    HttpEngineBridge.onRedirect(id, request, info, newLocationUrl);
  }

  @Override
  public void onSucceeded(UrlRequest request, UrlResponseInfo info) {
    HttpEngineBridge.onSucceeded(id, request, info);
  }

  @Override
  public void onFailed(UrlRequest request, UrlResponseInfo info, HttpException error) {
    HttpEngineBridge.onFailed(id, request, info, error);
  }

  @Override
  public void onCanceled(UrlRequest request, UrlResponseInfo info) {
    HttpEngineBridge.onCanceled(id, request, info);
  }
}
