/**
 * monet-rn demo app
 *
 * The JS battery screen: the same checks the Rust battery runs
 * (crates/monet-conformance), executed through the TS fetch layer against
 * the shared target — `cargo run -p monet-demo --bin server`. Same
 * routes, same assertions, JS host instead of Rust.
 *
 * Start the server first:
 *   cargo run -p monet-demo --bin server
 * iOS simulator defaults to 127.0.0.1; the Android emulator needs the
 * host's alias (10.0.2.2); a real device needs the host's LAN IP.
 *
 * @format
 */

import React, { useCallback, useEffect, useRef, useState} from 'react';
import {
  Platform,
  ScrollView,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';
import {SafeAreaView} from 'react-native-safe-area-context';

import {
  addRequestHook,
  addRequestObserver,
  addResponseHook,
  addResponseObserver,
  clearHooks,
  fetch,
  getMonet,
  MonetError,
  MonetWebSocket,
  MonetHookRequest,
  MonetHookResponse,
} from 'monet-rn';

type CheckStatus = 'pending' | 'pass' | 'fail';
type CheckState = {name: string; status: CheckStatus; detail: string};

const DEFAULT_BASE =
  Platform.OS === 'android' ? 'http://10.0.2.2:8964' : 'http://127.0.0.1:8964';
const DEFAULT_WS_BASE =
  Platform.OS === 'android' ? 'ws://10.0.2.2:8965' : 'ws://127.0.0.1:8965';
const CHECK_TIMEOUT_MS = 10_000;

function assert(condition: unknown, message: string): void {
  if (!condition) {
    throw new Error(message);
  }
}

/** Resolves when the socket's onopen fires (or rejects on error). */
function waitForOpen(ws: MonetWebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    ws.onopen = () => resolve();
    ws.onerror = (event: {kind: string; message: string}) =>
      reject(new MonetError(event.kind, event.message));
  });
}

/** Resolves with the next message event (text or binary). */
function nextMessage(
  ws: MonetWebSocket,
): Promise<{data: string | ArrayBuffer; isBinary: boolean}> {
  return new Promise((resolve, reject) => {
    ws.onmessage = event => {
      ws.onmessage = null;
      resolve(event);
    };
    ws.onerror = (event: {kind: string; message: string}) =>
      reject(new MonetError(event.kind, event.message));
  });
}

/** Resolves with the close event. */
function waitForClose(
  ws: MonetWebSocket,
): Promise<{code: number; reason: string; wasClean: boolean}> {
  return new Promise((resolve, reject) => {
    ws.onclose = event => {
      ws.onclose = null;
      resolve(event);
    };
    ws.onerror = (event: {kind: string; message: string}) =>
      reject(new MonetError(event.kind, event.message));
  });
}

async function withTimeout<T>(
  work: Promise<T>,
  label: string,
  timeoutMs: number = CHECK_TIMEOUT_MS,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error(`timed out after ${timeoutMs}ms (${label})`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

// ---- the checks (names mirror crates/monet-conformance/src/http.rs) ----

const CHECKS: Array<{
  name: string;
  /** Per-check timeout override; the default suits everything but the
   *  GC check, whose window is the collector's schedule. */
  timeoutMs?: number;
  run: (base: string) => Promise<void>;
}> = [
  {
    name: 'http:get_plain_body',
    run: async base => {
      const response = await fetch(`${base}/plain`);
      assert(response.status === 200, `status ${response.status}, expected 200`);
      const body = await response.text();
      assert(body === 'plain', `body ${JSON.stringify(body)}, expected "plain"`);
    },
  },
  {
    name: 'http:get_chunked_content',
    run: async base => {
      const response = await fetch(`${base}/chunked`);
      // Mirrors the Rust check: the concatenated content is the contract;
      // chunk boundaries are transport-dependent (URLSession legitimately
      // coalesces the small delayed chunks).
      const body = await response.text();
      assert(
        body === 'alphabetagamma',
        `body ${JSON.stringify(body)}, expected "alphabetagamma"`,
      );
    },
  },
  {
    name: 'http:post_reaches_server',
    run: async base => {
      const response = await fetch(`${base}/echo`, {
        method: 'POST',
        // Cronet-based transports (the platform HttpEngine) reject
        // uploads without a Content-Type; URLSession does not.
        headers: {'x-battery': 'post', 'content-type': 'text/plain'},
        body: 'hello-monet',
      });
      const seen = JSON.parse(await response.text()) as {
        method: string;
        body: string;
        headers: string[];
      };
      assert(seen.method === 'POST', `server saw method ${seen.method}`);
      assert(
        seen.body === 'hello-monet',
        `body ${JSON.stringify(seen.body)} did not survive the transport`,
      );
      assert(
        seen.headers.some(
          header => header.toLowerCase() === 'x-battery:post',
        ),
        'the x-battery header did not survive the transport',
      );
    },
  },
  {
    name: 'http:redirect_followed',
    run: async base => {
      const response = await fetch(`${base}/redirect`);
      assert(
        response.status === 200,
        `status ${response.status} — the redirect was not followed`,
      );
      const body = await response.text();
      assert(body === 'plain', `final body ${JSON.stringify(body)}`);
    },
  },
  {
    name: 'http:cancel_after_head_aborts_body',
    run: async base => {
      const controller = new AbortController();
      const response = await fetch(`${base}/stalled`, {
        signal: controller.signal,
      });
      let chunks = 0;
      let cancelled: MonetError | undefined;
      try {
        for await (const _chunk of response.body) {
          chunks++;
          controller.abort(); // mid-body, after the first chunk
        }
      } catch (error) {
        cancelled = error as MonetError;
      }
      assert(chunks >= 1, 'no chunk arrived before the abort');
      assert(
        cancelled !== undefined && cancelled.kind === 'Cancelled',
        cancelled === undefined
          ? 'the body completed — the abort did not take effect'
          : `expected kind Cancelled, got ${cancelled.kind}: ${cancelled.message}`,
      );
    },
  },
  {
    name: 'http:connection_refused',
    run: async _base => {
      try {
        await fetch('http://127.0.0.1:1/plain');
      } catch (error) {
        assert(
          error instanceof MonetError,
          `expected MonetError, got ${String(error)}`,
        );
        return;
      }
      throw new Error('a fetch to a closed port unexpectedly resolved');
    },
  },
  // ---- WebSocket checks (names mirror monet-conformance/src/ws.rs) ----
  {
    name: 'ws:round_trips',
    run: async _base => {
      const ws = new MonetWebSocket(`${DEFAULT_WS_BASE}/echo`);
      try {
        await waitForOpen(ws);
        // Text round-trip: send first, then wait for the echo.
        ws.send('hello-ws');
        const text = await nextMessage(ws);
        assert(
          text.data === 'hello-ws',
          `text echo ${JSON.stringify(text.data)}, expected "hello-ws"`,
        );
        // Binary round-trip.
        const payload = new Uint8Array([0, 1, 2, 254, 255]);
        ws.send(payload.buffer);
        const binary = await nextMessage(ws);
        assert(
          binary.data instanceof ArrayBuffer,
          `expected ArrayBuffer, got ${typeof binary.data}`,
        );
        const echoed = new Uint8Array(binary.data as ArrayBuffer);
        assert(
          echoed.length === payload.length &&
            echoed.every((byte, i) => byte === payload[i]),
          `binary echo ${Array.from(echoed)}, expected ${Array.from(payload)}`,
        );
      } finally {
        ws.close(1000, 'done');
        await waitForClose(ws);
      }
    },
  },
  {
    name: 'ws:close_code_fidelity',
    run: async _base => {
      const ws = new MonetWebSocket(`${DEFAULT_WS_BASE}/close-1012`);
      const close = await waitForClose(ws);
      assert(
        close.code === 1012,
        `close code ${close.code}, expected 1012`,
      );
      assert(
        close.reason === 'service restart',
        `close reason ${JSON.stringify(close.reason)}, expected "service restart"`,
      );
      assert(close.wasClean, 'the close was not clean');
    },
  },
  {
    name: 'ws:ping_surfaced',
    run: async _base => {
      const ws = new MonetWebSocket(`${DEFAULT_WS_BASE}/ping`);
      try {
        await waitForOpen(ws);
        // The server sends a ping; the transport answers it and surfaces
        // it to the user.
        const ping = await nextMessage(ws);
        assert(
          ping.isBinary,
          `expected the ping to surface as binary, got isBinary=${ping.isBinary}`,
        );
        // The close semantics after a ping/pong exchange are handled by
        // the close_code_fidelity test; this check is about surfacing.
      } finally {
        ws.abort();
      }
    },
  },
  {
    name: 'ws:dropped_socket_collectable',
    timeoutMs: 30_000,
    run: async base => {
      // The JS side of the release contract: a socket dropped without
      // close()/abort() must be collectable — nothing on this side may
      // retain it (the event closures hold only a bare id, the connecting
      // entry is released on open). The Rust teardown (NativeState drop →
      // cancel → pump exit) follows from the same collection; its timing
      // is the collector's — Hades runs NativeState finalizers on natural
      // old-generation sweeps only (a forced gc() skips them via the
      // force-compaction path), so the connection close is an
      // eventually-observable, not something this window can pin down.
      // The framework-level guarantee is pinned by the host ground-truth
      // tests: jsi-tests native_state_gc / turbo_object's
      // turbo_object_instance_is_reclaimed_by_gc.
      const openCount = async (): Promise<number> =>
        Number(await (await fetch(`${base}/ws-open`)).text());
      let ws: MonetWebSocket | undefined = new MonetWebSocket(
        `${DEFAULT_WS_BASE}/gc-probe`,
      );
      await waitForOpen(ws);
      const probe = new WeakRef(ws);
      // The server counts the connection when its handshake completes —
      // microseconds after the client's on_open. A single immediate read
      // races that arming (observed on iOS); poll briefly instead.
      for (let i = 0; (await openCount()) === 0; i++) {
        if (i > 40) {
          throw new Error('the server did not count the open /gc-probe connection');
        }
        await new Promise<void>(resolve => setTimeout(resolve, 50));
      }
      // Drop the only reference. The WeakRef was built on the local, but
      // construction does not enter Hermes' keep-alive set (only deref of
      // a live target does), and no event dispatch happens after the
      // drop on this idle echo connection.
      ws = undefined;
      // Natural allocation pressure first (production-shaped GC), then
      // explicit collections to settle the WeakRef (gc() clears weak
      // references reliably — it is only cell finalization it skips).
      const live: unknown[][] = [];
      for (let round = 0; round < 12; round++) {
        live.push(new Array(50_000).fill(round));
        if (live.length >= 40) {
          live.length = 0;
        }
        await new Promise<void>(resolve => setTimeout(resolve, 100));
      }
      live.length = 0;
      const gc = (globalThis as {gc?: () => void}).gc;
      gc?.();
      gc?.();
      await new Promise<void>(resolve => setTimeout(resolve, 300));
      assert(
        probe.deref() === undefined,
        'the dropped socket was still reachable — something on the JS side retains it',
      );
    },
  },
  // ---- hook/observer checks (names mirror docs/plans/2026-11-hooks.md).
  // Every check clears the global registrations in a finally — the checks
  // run sequentially in one process and would otherwise pollute each
  // other. ----
  {
    name: 'http:observer_zero_impact',
    run: async base => {
      const observed: MonetHookRequest[] = [];
      try {
        // A hook stamps the request; the observer must see the FINAL form
        // (post-hook, pre-wire) while the fetch proceeds completely
        // unaffected by being observed.
        addRequestObserver(req => {
          observed.push(req);
        });
        addRequestHook(req => ({
          ...req,
          headers: [...req.headers, {name: 'x-observer-test', value: 'stamped'}],
        }));
        const response = await fetch(`${base}/plain`);
        assert(
          response.status === 200,
          `the observed fetch failed (status ${response.status})`,
        );
        const body = await response.text();
        assert(body === 'plain', `body ${JSON.stringify(body)}`);
        assert(observed.length === 1, `observer fired ${observed.length} times`);
        assert(
          observed[0].headers.some(
            header => header.name === 'x-observer-test' && header.value === 'stamped',
          ),
          'the observer did not see the hook-stamped header — not the final form',
        );
      } finally {
        clearHooks();
      }
    },
  },
  {
    name: 'http:request_hook_rewrites_header',
    run: async base => {
      try {
        addRequestHook(req => ({
          ...req,
          headers: [...req.headers, {name: 'x-hook', value: 'yes'}],
        }));
        const response = await fetch(`${base}/echo`);
        const seen = JSON.parse(await response.text()) as {headers: string[]};
        assert(
          seen.headers.some(header => header.toLowerCase() === 'x-hook:yes'),
          'the hook-added header did not reach the server',
        );
      } finally {
        clearHooks();
      }
    },
  },
  {
    name: 'http:hook_error_fails_fetch',
    run: async base => {
      try {
        addRequestHook(() => {
          throw new Error('hook veto');
        });
        try {
          await fetch(`${base}/plain`);
        } catch (error) {
          assert(
            error instanceof MonetError && error.kind === 'Hook',
            error instanceof MonetError
              ? `expected kind Hook, got ${error.kind}: ${error.message}`
              : `expected MonetError, got ${String(error)}`,
          );
          return;
        }
        throw new Error('a fetch with a throwing request hook resolved');
      } finally {
        clearHooks();
      }
    },
  },
  {
    name: 'http:hook_async_resolution',
    run: async base => {
      try {
        // The hook awaits before rewriting — the pipeline must park on
        // the pending-map resolution, not race ahead with the original
        // request.
        addRequestHook(async req => {
          await new Promise<void>(resolve => setTimeout(resolve, 50));
          return {
            ...req,
            headers: [...req.headers, {name: 'x-async-hook', value: 'waited'}],
          };
        });
        const response = await fetch(`${base}/echo`);
        const seen = JSON.parse(await response.text()) as {headers: string[]};
        assert(
          seen.headers.some(header => header.toLowerCase() === 'x-async-hook:waited'),
          'the async hook rewrite did not reach the server',
        );
      } finally {
        clearHooks();
      }
    },
  },
  {
    name: 'http:hook_retry_uses_new_token',
    run: async base => {
      try {
        // The token-refresh shape end to end: the request hook injects
        // from a shared store, the response hook sees the 401, "refreshes"
        // (flips the store), and asks for a retry — the re-run picks up
        // the new token and /auth-guard accepts it. The attempt counter
        // must read 1 on the second run.
        let token = 'token1';
        let retryAttempt = -1;
        addRequestHook(req => ({
          ...req,
          headers: [
            ...req.headers.filter(
              header => header.name.toLowerCase() !== 'authorization',
            ),
            {name: 'Authorization', value: `Bearer ${token}`},
          ],
        }));
        addResponseHook((res, ctx) => {
          if (res.status === 401) {
            retryAttempt = ctx.attempt;
            token = 'token2'; // the "refresh"
            return {retry: true};
          }
          return res;
        });
        const response = await fetch(`${base}/auth-guard`);
        assert(response.status === 200, `status ${response.status} after the retry`);
        const body = await response.text();
        assert(body === 'ok', `body ${JSON.stringify(body)}`);
        assert(
          retryAttempt === 0,
          `the retry fired at attempt ${retryAttempt}, expected 0`,
        );
      } finally {
        clearHooks();
      }
    },
  },
  {
    name: 'http:hook_retry_gives_up',
    run: async _base => {
      try {
        // A response hook that always retries must hit the module's hard
        // cap and fail the fetch — not loop forever.
        addResponseHook(_res => ({retry: true}));
        try {
          await fetch('http://127.0.0.1:1/plain');
        } catch {
          // Any terminal error (Hook or Transport) proves the cap ended
          // the loop; withTimeout guards the "not hang" property.
          return;
        }
        throw new Error('an always-retrying hook never terminated the fetch');
      } finally {
        clearHooks();
      }
    },
  },
  {
    name: 'http:abort_during_hook',
    run: async base => {
      try {
        // A hook that never resolves parks the pipeline; aborting the
        // fetch must win the race with kind Cancelled. The dangling hook
        // call is retired by clearHooks in the finally.
        addRequestHook(
          () =>
            new Promise<MonetHookRequest>(() => {
              // Never resolves — the abort is the only way out.
            }),
        );
        const controller = new AbortController();
        const pending = fetch(`${base}/plain`, {signal: controller.signal});
        await new Promise<void>(resolve => setTimeout(resolve, 100));
        controller.abort();
        try {
          await pending;
        } catch (error) {
          assert(
            error instanceof MonetError && error.kind === 'Cancelled',
            error instanceof MonetError
              ? `expected kind Cancelled, got ${error.kind}: ${error.message}`
              : `expected MonetError, got ${String(error)}`,
          );
          return;
        }
        throw new Error('the abort during a parked hook did not cancel the fetch');
      } finally {
        clearHooks();
      }
    },
  },
  {
    name: 'ws:request_hook_headers',
    run: async _base => {
      try {
        // The same request-hook chain runs on the WebSocket upgrade
        // request: the header it injects must arrive at the server, which
        // /hook-echo reflects back as the first message.
        addRequestHook(req => ({
          ...req,
          headers: [...req.headers, {name: 'x-ws-hook', value: 'injected'}],
        }));
        const ws = new MonetWebSocket(`${DEFAULT_WS_BASE}/hook-echo`);
        try {
          await waitForOpen(ws);
          const first = await nextMessage(ws);
          assert(
            first.data === 'injected',
            `first message ${JSON.stringify(first.data)}, expected "injected"`,
          );
        } finally {
          ws.close(1000, 'done');
          await waitForClose(ws);
        }
      } finally {
        clearHooks();
      }
    },
  },
  {
    name: 'http:metadata_flow_tracking',
    run: async base => {
      // The bank-flow scenario end to end: a series of requests (a, b, d,
      // c — completion order varies under Promise.all) tagged with one
      // call-site flowId plus a step. The observers must reconstruct the
      // flow: every event carries the flowId, the correlationId pairs
      // each request with its own response (different fetches never
      // cross-pair), and the response side sees the same metadata the
      // request side saw (extensions threading).
      type Event = {
        phase: 'req' | 'res';
        step: string;
        correlationId: number;
        status?: number;
      };
      const events: Event[] = [];
      try {
        addRequestObserver((_req, ctx) => {
          events.push({
            phase: 'req',
            step: ctx.metadata.step,
            correlationId: ctx.correlationId,
          });
        });
        addResponseObserver((res, ctx) => {
          events.push({
            phase: 'res',
            step: ctx.metadata.step,
            correlationId: ctx.correlationId,
            status: res.status,
          });
        });
        const flow = {flowId: 'account-open-1'};
        const results = await Promise.all(
          ['a', 'b', 'd', 'c'].map(async step => {
            const response = await fetch(`${base}/plain`, {
              metadata: {...flow, step},
            });
            return {step, status: response.status};
          }),
        );
        for (const result of results) {
          assert(
            result.status === 200,
            `step ${result.step} failed (status ${result.status})`,
          );
        }
        assert(events.length === 8, `saw ${events.length} events, expected 8`);
        // Every event carries the flow's steps.
        const steps = new Set(events.map(event => event.step));
        assert(
          steps.size === 4 && ['a', 'b', 'c', 'd'].every(s => steps.has(s)),
          `flow steps ${[...steps].sort()}, expected all of a/b/c/d`,
        );
        // Every request pairs with exactly one response of the SAME step
        // and correlationId (completion order may interleave — pairing
        // must not).
        for (const step of ['a', 'b', 'c', 'd']) {
          const mine = events.filter(event => event.step === step);
          assert(
            mine.length === 2,
            `step ${step} saw ${mine.length} events, expected 2`,
          );
          const [req, res] = mine;
          assert(req.phase === 'req' && res.phase === 'res', `step ${step} phases`);
          assert(
            req.correlationId === res.correlationId,
            `step ${step} correlation mismatch: ${req.correlationId} vs ${res.correlationId}`,
          );
        }
        // Different fetches have different correlationIds.
        const ids = new Set(events.map(event => event.correlationId));
        assert(ids.size === 4, `correlationIds ${ids.size}, expected 4 distinct`);
      } finally {
        clearHooks();
      }
    },
  },
];

function App(): React.JSX.Element {
  const [rows, setRows] = useState<CheckState[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [base, setBase] = useState(DEFAULT_BASE);
  const runningRef = useRef(false);

  const runBattery = useCallback(async (baseUrl: string) => {
    if (runningRef.current) {
      return;
    }
    runningRef.current = true;
    const trimmed = baseUrl.replace(/\/+$/, '');
    setRows(CHECKS.map(check => ({name: check.name, status: 'pending', detail: ''})));
    setError(null);
    try {
      for (let i = 0; i < CHECKS.length; i++) {
        const check = CHECKS[i];
        const started = Date.now();
        try {
          await withTimeout(
            check.run(trimmed),
            check.name,
            check.timeoutMs ?? CHECK_TIMEOUT_MS,
          );
          setRows(previous => previous.map((row, index) =>
            index === i
              ? {...row, status: 'pass', detail: `${Date.now() - started}ms`}
              : row,
          ));
        } catch (failure) {
          setRows(previous => previous.map((row, index) =>
            index === i
              ? {
                  ...row,
                  status: 'fail',
                  detail: failure instanceof Error ? failure.message : String(failure),
                }
              : row,
          ));
        }
      }
    } catch (setupFailure) {
      setError(String(setupFailure));
    } finally {
      runningRef.current = false;
    }
  }, []);

  useEffect(() => {
    runBattery(DEFAULT_BASE);
  }, [runBattery]);

  const monet = useRef<{ping: string; transportStack: string} | null>(null);
  if (monet.current === null) {
    try {
      const module = getMonet();
      monet.current = {
        ping: module.ping(),
        transportStack: module.transportStack,
      };
    } catch (moduleError) {
      monet.current = {
        ping: `error: ${String(moduleError)}`,
        transportStack: 'unavailable',
      };
    }
  }

  const passed = rows.filter(row => row.status === 'pass').length;
  const failed = rows.filter(row => row.status === 'fail').length;

  return (
    <SafeAreaView style={styles.container}>
      <StatusBar barStyle="dark-content" />
      <ScrollView contentContainerStyle={styles.content}>
        <Text style={styles.title}>monet-rn</Text>
        <View style={styles.row}>
          <Text style={styles.key}>Monet.ping()</Text>
          <Text style={styles.value}>{monet.current.ping}</Text>
        </View>
        <View style={styles.row}>
          <Text style={styles.key}>Monet.transportStack</Text>
          <Text style={styles.value}>{monet.current.transportStack}</Text>
        </View>
        <Text style={styles.section}>JS battery — HTTP</Text>
        <TextInput
          style={styles.input}
          value={base}
          onChangeText={setBase}
          autoCapitalize="none"
          autoCorrect={false}
          keyboardType="url"
          placeholder="http://host:port"
        />
        <View style={styles.controls}>
          <Text style={styles.summary}>
            {rows.length === 0 ? '' : `${passed} pass · ${failed} fail`}
          </Text>
          <Text style={styles.runButton} onPress={() => runBattery(base)}>
            run
          </Text>
        </View>
        {error !== null && <Text style={styles.error}>error: {error}</Text>}
        {rows.map(row => (
          <View key={row.name} style={styles.checkRow}>
            <Text
              style={[
                styles.checkName,
                row.status === 'pass' && styles.pass,
                row.status === 'fail' && styles.fail,
              ]}>
              {row.name}
            </Text>
            {row.detail !== '' && (
              <Text style={styles.checkDetail} numberOfLines={3}>
                {row.detail}
              </Text>
            )}
          </View>
        ))}
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {flex: 1, backgroundColor: 'white'},
  content: {padding: 16},
  title: {fontSize: 22, fontWeight: '700', marginBottom: 16},
  row: {marginBottom: 12},
  key: {fontSize: 13, color: '#666', fontFamily: 'Menlo'},
  value: {fontSize: 15, fontFamily: 'Menlo'},
  section: {
    fontSize: 15,
    fontWeight: '600',
    marginTop: 12,
    marginBottom: 8,
  },
  input: {
    borderWidth: 1,
    borderColor: '#ccc',
    borderRadius: 6,
    padding: 8,
    fontFamily: 'Menlo',
    fontSize: 13,
    color: '#333',
  },
  controls: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginTop: 8,
    marginBottom: 12,
  },
  summary: {fontSize: 13, color: '#666', fontFamily: 'Menlo'},
  runButton: {color: '#007aff', fontSize: 15, fontWeight: '600'},
  error: {color: 'red', fontFamily: 'Menlo', fontSize: 12, marginBottom: 8},
  checkRow: {marginBottom: 10},
  checkName: {fontSize: 14, fontFamily: 'Menlo', color: '#999'},
  checkDetail: {fontSize: 12, fontFamily: 'Menlo', color: '#b00'},
  pass: {color: '#070'},
  fail: {color: '#b00'},
});

export default App;
