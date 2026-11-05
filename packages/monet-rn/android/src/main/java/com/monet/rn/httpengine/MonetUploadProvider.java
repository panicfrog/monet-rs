// Buffered-upload provider (v1): android.net.http ships no
// UploadDataProviders helper, so the byte-array adapter lives here.
package com.monet.rn.httpengine;

import android.net.http.UploadDataProvider;
import android.net.http.UploadDataSink;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class MonetUploadProvider extends UploadDataProvider {
  private final byte[] data;
  private int offset = 0;

  public MonetUploadProvider(byte[] data) {
    this.data = data;
  }

  @Override
  public long getLength() {
    return data.length;
  }

  @Override
  public void read(UploadDataSink sink, ByteBuffer buffer) throws IOException {
    int count = Math.min(buffer.remaining(), data.length - offset);
    buffer.put(data, offset, count);
    offset += count;
    sink.onReadSucceeded(false);
  }

  @Override
  public void rewind(UploadDataSink sink) throws IOException {
    offset = 0;
    sink.onRewindSucceeded();
  }
}
