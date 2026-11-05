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

import {fetch, getMonet, MonetError} from 'monet-rn';

type CheckStatus = 'pending' | 'pass' | 'fail';
type CheckState = {name: string; status: CheckStatus; detail: string};

const DEFAULT_BASE =
  Platform.OS === 'android' ? 'http://10.0.2.2:8964' : 'http://127.0.0.1:8964';
const CHECK_TIMEOUT_MS = 10_000;

function assert(condition: unknown, message: string): void {
  if (!condition) {
    throw new Error(message);
  }
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
        headers: {'x-battery': 'post'},
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
