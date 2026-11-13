package com.monet.demo.httpengine

import android.net.http.HttpException
import android.net.http.UploadDataProvider
import android.net.http.UploadDataSink
import android.net.http.UrlRequest
import android.net.http.UrlResponseInfo
import java.nio.ByteBuffer
import java.util.concurrent.Executor

/**
 * The mechanical Kotlin bridge for monet-httpengine: forwards Cronet's
 * Java callbacks into the native crate with the request id. All state
 * lives on the Rust side (the inverted shape of monet-urlsession's
 * Swift shim — the shim language stays mechanical).
 */
object HttpEngineBridge {
    init {
        // The demo app loads the shared library once (MainActivity); this
        // object only needs the symbols to resolve.
        System.loadLibrary("monet_demo")
    }

    @JvmStatic external fun onStarted(id: Long, request: UrlRequest, info: UrlResponseInfo)
    @JvmStatic external fun onReadCompleted(
        id: Long, request: UrlRequest, info: UrlResponseInfo, byteBuffer: ByteBuffer)
    @JvmStatic external fun onRedirect(
        id: Long, request: UrlRequest, info: UrlResponseInfo, location: String)
    @JvmStatic external fun onSucceeded(id: Long, request: UrlRequest, info: UrlResponseInfo)
    @JvmStatic external fun onFailed(
        id: Long, request: UrlRequest, info: UrlResponseInfo?, error: HttpException)
    @JvmStatic external fun onCanceled(id: Long, request: UrlRequest, info: UrlResponseInfo?)

    // The chunked upload provider's reach (v2/W2d): fills `buffer`'s
    // [position, position + remaining) window from the Rust feeder's
    // queue — > 0 bytes written, 0 the end, -1 nothing yet (the read
    // parks), -2 the stream failed.
    @JvmStatic external fun takeUploadChunk(
        id: Long, buffer: ByteBuffer, position: Int, remaining: Int): Int

    // The provider's close: ends the Rust-side upload state.
    @JvmStatic external fun closeUpload(id: Long)
}

/** Forwards every callback to [HttpEngineBridge] with the request id. */
class MonetRequestCallback(private val id: Long) : UrlRequest.Callback {
    override fun onResponseStarted(request: UrlRequest, info: UrlResponseInfo) {
        HttpEngineBridge.onStarted(id, request, info)
    }

    override fun onReadCompleted(
        request: UrlRequest, info: UrlResponseInfo, byteBuffer: ByteBuffer
    ) {
        HttpEngineBridge.onReadCompleted(id, request, info, byteBuffer)
    }

    override fun onRedirectReceived(
        request: UrlRequest, info: UrlResponseInfo, newLocationUrl: String
    ) {
        HttpEngineBridge.onRedirect(id, request, info, newLocationUrl)
    }

    override fun onSucceeded(request: UrlRequest, info: UrlResponseInfo) {
        HttpEngineBridge.onSucceeded(id, request, info)
    }

    override fun onFailed(request: UrlRequest, info: UrlResponseInfo?, error: HttpException) {
        HttpEngineBridge.onFailed(id, request, info, error)
    }

    override fun onCanceled(request: UrlRequest, info: UrlResponseInfo?) {
        HttpEngineBridge.onCanceled(id, request, info)
    }
}

/**
 * Buffered-upload provider (v1): android.net.http ships no
 * UploadDataProviders helper, so the byte-array adapter lives here.
 */
class MonetUploadProvider(private val data: ByteArray) : UploadDataProvider() {
    private var offset = 0

    override fun getLength(): Long = data.size.toLong()

    override fun read(sink: UploadDataSink, buffer: ByteBuffer) {
        val count = minOf(buffer.remaining(), data.size - offset)
        buffer.put(data, offset, count)
        offset += count
        sink.onReadSucceeded(false)
    }

    override fun rewind(sink: UploadDataSink) {
        offset = 0
        sink.onRewindSucceeded()
    }
}

/**
 * Streaming-upload provider (v2/W2d): the chunked shape — getLength()
 * answers -1 (the length is unknown ahead), reads pull from the Rust
 * feeder's queue and park when it's empty, and rewind refuses (a
 * stream cannot replay; redirects that preserve the body fail instead
 * of re-sending). The Rust side (monet-httpengine's upload module) is
 * the state machine; this class is the mechanical half.
 */
class MonetStreamedProvider(private val id: Long) : UploadDataProvider() {
    private var pendingSink: UploadDataSink? = null
    private var pendingBuffer: ByteBuffer? = null
    private var closed = false

    override fun getLength(): Long = -1L

    @Synchronized
    override fun read(sink: UploadDataSink, buffer: ByteBuffer) {
        if (!drain(sink, buffer)) {
            // Nothing yet: hold the read; the feeder's notify
            // (onDataAvailable) completes it — the asynchronous
            // completion the UploadDataProvider contract allows.
            pendingSink = sink
            pendingBuffer = buffer
        }
    }

    /**
     * Wakes a parked read — called over JNI from the Rust feeder thread.
     * A notify can be stale: the feeder fires one per push, and reads
     * issued in between may already have taken that chunk. An empty take
     * re-parks the read — dropping it would leave Cronet's read
     * outstanding with nobody left to complete it, hanging the upload.
     * The slot is cleared BEFORE the drain: a completing drain calls into
     * the sink, which may issue the next read() re-entrantly (same
     * thread, reentrant monitor) and park it here.
     */
    @Synchronized
    fun onDataAvailable() {
        val sink = pendingSink ?: return
        val buffer = pendingBuffer ?: return
        pendingSink = null
        pendingBuffer = null
        if (!drain(sink, buffer)) {
            pendingSink = sink
            pendingBuffer = buffer
        }
    }

    // One take attempt; returns true when the read completed. Callers
    // hold this object's monitor (read/onDataAvailable).
    private fun drain(sink: UploadDataSink, buffer: ByteBuffer): Boolean {
        if (buffer.remaining() == 0) { // defensive: Cronet asks with room
            sink.onReadError(java.io.IOException("upload read with a full buffer"))
            return true
        }
        val position = buffer.position()
        val count = HttpEngineBridge.takeUploadChunk(id, buffer, position, buffer.remaining())
        if (count > 0) {
            buffer.position(position + count)
            sink.onReadSucceeded(false)
            return true
        }
        return when (count) {
            0 -> { // the stream ended: the final, empty chunk
                sink.onReadSucceeded(true); true
            }
            -1 -> false // nothing yet: park
            else -> { // -2: the producer's stream failed
                sink.onReadError(java.io.IOException("upload body failed")); true
            }
        }
    }

    @Synchronized
    override fun rewind(sink: UploadDataSink) {
        // A stream cannot replay; onRewindError is the documented answer.
        sink.onRewindError(java.io.IOException("a streamed upload cannot be rewound"))
    }

    @Synchronized
    override fun close() {
        if (!closed) {
            closed = true
            HttpEngineBridge.closeUpload(id)
        }
    }
}

/**
 * Direct executor: Cronet invokes callbacks on its own threads, which
 * are already attached — paired with setDirectExecutorAllowed(true).
 */
class MonetDirectExecutor : Executor {
    override fun execute(command: Runnable) {
        command.run()
    }
}
