package com.monet.demo

import android.app.Activity
import android.os.Bundle
import android.util.Log
import android.widget.ScrollView
import android.widget.TextView

/**
 * Runs the monet-rs batteries on device: the identical checks the host
 * integration tests run, over loopback servers inside this process —
 * plus the platform HttpEngine battery (system Cronet, API 34+).
 * Reports land on screen and in logcat (tag MonetDemo).
 */
class MainActivity : Activity() {

    private external fun runBattery(): String
    private external fun initPlatformVerifier(context: android.content.Context): String
    private external fun runPlatformBattery(): String
    private external fun initHttpEngine(context: android.content.Context): String
    private external fun runHttpEngineBattery(): String
    private external fun httpengineHeaderProbe(): String

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val report = buildString {
            append(section("battery") { runBattery() })
            append(section("platform-verifier init") { initPlatformVerifier(this@MainActivity) })
            append(section("platform-verifier battery") { runPlatformBattery() })
            append(section("httpengine init") { initHttpEngine(this@MainActivity) })
            append(section("httpengine probe") { httpengineHeaderProbe() })
            append(section("httpengine battery") { runHttpEngineBattery() })
        }
        Log.i(TAG, "battery result:\n$report")
        val text = TextView(this).apply {
            text = report
            setTextIsSelectable(true)
            setPadding(48, 48, 48, 48)
            textSize = 12f
        }
        setContentView(ScrollView(this).apply { addView(text) })
    }

    private inline fun section(name: String, run: () -> String): String {
        val body = try {
            run()
        } catch (t: Throwable) {
            "crashed: $t"
        }
        return "=== $name ===\n$body\n\n"
    }

    companion object {
        private const val TAG = "MonetDemo"

        init {
            System.loadLibrary("monet_demo")
        }
    }
}
