// Streaming-upload provider (v2/W2d): android.net.http's chunked shape —
// getLength() answers -1 (the length is unknown ahead), reads pull from
// the Rust feeder's queue and park when it's empty, and rewind refuses
// (a stream cannot replay; redirects that preserve the body fail
// instead of re-sending). The Rust side (monet-httpengine's upload
// module) is the state machine; this class is the mechanical half.
package com.monet.rn.httpengine;

import android.net.http.UploadDataProvider;
import android.net.http.UploadDataSink;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class MonetStreamedProvider extends UploadDataProvider {
  private final long id;
  private UploadDataSink pendingSink;
  private ByteBuffer pendingBuffer;
  private boolean closed;

  public MonetStreamedProvider(long id) {
    this.id = id;
  }

  @Override
  public long getLength() {
    return -1L;
  }

  @Override
  public synchronized void read(UploadDataSink sink, ByteBuffer buffer) throws IOException {
    if (!drain(sink, buffer)) {
      // Nothing yet: hold the read; the feeder's notify (onDataAvailable)
      // completes it. The UploadDataProvider contract allows exactly this
      // asynchronous completion.
      pendingSink = sink;
      pendingBuffer = buffer;
    }
  }

  // Wakes a parked read — called over JNI from the Rust feeder thread
  // after a chunk (or a terminal) lands in the queue.
  public synchronized void onDataAvailable() {
    UploadDataSink sink = pendingSink;
    ByteBuffer buffer = pendingBuffer;
    if (sink != null && buffer != null) {
      pendingSink = null;
      pendingBuffer = null;
      drain(sink, buffer);
    }
  }

  // One take attempt; returns true when the read completed. Callers hold
  // this object's monitor (read/onDataAvailable).
  private boolean drain(UploadDataSink sink, ByteBuffer buffer) {
    if (buffer.remaining() == 0) { // defensive: Cronet asks with room
      sink.onReadError(new IOException("upload read with a full buffer"));
      return true;
    }
    int position = buffer.position();
    int count =
        HttpEngineBridge.takeUploadChunk(id, buffer, position, buffer.remaining());
    if (count > 0) {
      buffer.position(position + count);
      sink.onReadSucceeded(false);
      return true;
    }
    if (count == 0) { // the stream ended: the final, empty chunk
      sink.onReadSucceeded(true);
      return true;
    }
    if (count == -1) { // nothing yet: park
      return false;
    }
    // -2: the producer's stream failed
    sink.onReadError(new IOException("upload body failed"));
    return true;
  }

  @Override
  public synchronized void rewind(UploadDataSink sink) throws IOException {
    // A stream cannot replay; onRewindError is the documented answer.
    sink.onRewindError(new IOException("a streamed upload cannot be rewound"));
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      HttpEngineBridge.closeUpload(id);
    }
  }
}
