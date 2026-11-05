// Direct executor: Cronet invokes callbacks on its own threads, which
// are already attached — paired with setDirectExecutorAllowed(true) on
// the request builder.
package com.monet.rn.httpengine;

import java.util.concurrent.Executor;

public final class MonetDirectExecutor implements Executor {
  @Override
  public void execute(Runnable command) {
    command.run();
  }
}
