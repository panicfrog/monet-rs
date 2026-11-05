// The autolinking package: exists to run the HttpEngine bootstrap at app
// startup (createNativeModules is called while the React context is
// built — well before any JS fetch). Contributes no modules or views of
// its own.
package com.monet.rn;

import com.facebook.react.ReactPackage;
import com.facebook.react.bridge.NativeModule;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.uimanager.ViewManager;
import com.monet.rn.httpengine.MonetBootstrap;
import java.util.Collections;
import java.util.List;

public class MonetRnPackage implements ReactPackage {
  @Override
  public List<NativeModule> createNativeModules(ReactApplicationContext reactContext) {
    MonetBootstrap.init(reactContext);
    return Collections.emptyList();
  }

  @Override
  public List<ViewManager> createViewManagers(ReactApplicationContext reactContext) {
    return Collections.emptyList();
  }
}
