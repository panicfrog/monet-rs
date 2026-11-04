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
 * Direct executor: Cronet invokes callbacks on its own threads, which
 * are already attached — paired with setDirectExecutorAllowed(true).
 */
class MonetDirectExecutor : Executor {
    override fun execute(command: Runnable) {
        command.run()
    }
}
