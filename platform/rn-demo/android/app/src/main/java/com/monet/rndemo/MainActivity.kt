package com.monet.rndemo

import android.os.Bundle
import com.facebook.react.ReactActivity
import com.facebook.react.ReactActivityDelegate
import com.facebook.react.defaults.DefaultNewArchitectureEntryPoint.fabricEnabled
import com.facebook.react.defaults.DefaultReactActivityDelegate
import com.monet.rn.httpengine.MonetBootstrap

class MainActivity : ReactActivity() {

  /**
   * The HttpEngine bootstrap: hands the JVM + app Context to the Rust
   * side before any fetch can run. The ReactPackage auto-hook does not
   * fire under bridgeless, so the explicit call is the reliable path
   * (idempotent — a second call from the package hook would be a no-op).
   */
  override fun onCreate(savedInstanceState: Bundle?) {
    super.onCreate(savedInstanceState)
    MonetBootstrap.init(this)
  }

  /**
   * Returns the name of the main component registered from JavaScript. This is used to schedule
   * rendering of your application.
   */
  override fun getMainComponentName(): String = "MonetDemo"

  /**
   * Returns the instance of the [ReactActivityDelegate]. We use [DefaultReactActivityDelegate]
   * which allows you to enable New Architecture with a single boolean flags [fabricEnabled]
   */
  override fun createReactActivityDelegate(): ReactActivityDelegate =
      DefaultReactActivityDelegate(this, mainComponentName, fabricEnabled)
}
