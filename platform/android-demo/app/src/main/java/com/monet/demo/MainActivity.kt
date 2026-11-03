package com.monet.demo

import android.app.Activity
import android.os.Bundle
import android.util.Log
import android.widget.ScrollView
import android.widget.TextView

/**
 * Runs the monet-rs battery on device: the identical checks the host
 * integration test runs, over loopback servers inside this process.
 * The report lands on screen and in logcat (tag MonetDemo).
 */
class MainActivity : Activity() {

    private external fun runBattery(): String

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val report = try {
            runBattery()
        } catch (t: Throwable) {
            "battery crashed: $t"
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

    companion object {
        private const val TAG = "MonetDemo"

        init {
            System.loadLibrary("monet_demo")
        }
    }
}
