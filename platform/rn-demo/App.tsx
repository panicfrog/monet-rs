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
  SafeAreaView,
  ScrollView,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';

import {fetch, getMonet, MonetError, MonetWebSocket} from 'monet-rn';

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

async function withTimeout<T>(work: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error(`timed out after ${CHECK_TIMEOUT_MS}ms`)),
          CHECK_TIMEOUT_MS,
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

const CHECKS: Array<{name: string; run: (base: string) => Promise<void>}> = [
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
          await withTimeout(check.run(trimmed), check.name);
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
