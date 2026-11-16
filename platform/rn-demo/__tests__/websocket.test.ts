/**
 * MonetWebSocket's message pairing, against a fake native connection.
 *
 * The native pump queues each message, then signals `on_message` with its
 * isBinary flag; JS pulls the data with `takeMessage`. Signals reach JS
 * later than the queue push, so JS routinely sees a backlog — messages
 * whose signals haven't arrived yet sit in the queue behind the current
 * one. The fake reproduces exactly that.
 */

import {utf8Encode} from '../../../packages/monet-rn/ts/utf8';
import {MonetWebSocket} from '../../../packages/monet-rn/ts/websocket';

type Signal = (event: any) => void;

const native = {
  queue: [] as ArrayBuffer[],
  onOpen: null as Signal | null,
  onMessage: null as Signal | null,
};

// getMonet() reads the binding the native side installs on
// globalThis.RustTurbo; install a fake Monet there instead.
const fakeMonet = {
  wsConnect: (
    _url: string,
    _protocols: string[],
    _headers: unknown[],
    onOpen: Signal,
    onMessage: Signal,
  ) => {
    native.onOpen = onOpen;
    native.onMessage = onMessage;
    return {
      send: () => true,
      sendPing: () => true,
      close: () => true,
      abort: () => true,
      // The native receive queue is FIFO.
      takeMessage: () => native.queue.shift() ?? null,
    };
  },
};
(globalThis as any).RustTurbo = {get: () => fakeMonet};

function bytes(...values: number[]): ArrayBuffer {
  return new Uint8Array(values).buffer;
}

function text(value: string): ArrayBuffer {
  return utf8Encode(value);
}

beforeEach(() => {
  native.queue = [];
  native.onOpen = null;
  native.onMessage = null;
});

test('a backlog keeps order and each message keeps its own type', () => {
  const ws = new MonetWebSocket('ws://test/burst');
  const received: {data: string | ArrayBuffer; isBinary: boolean}[] = [];
  ws.onmessage = event => received.push(event);
  native.onOpen!({});

  // Three messages land natively before JS runs the first signal.
  native.queue.push(text('m0'), bytes(1), text('m2'));
  native.onMessage!({isBinary: false});
  native.onMessage!({isBinary: true});
  native.onMessage!({isBinary: false});

  expect(received.map(event => event.isBinary)).toEqual([false, true, false]);
  expect(received[0].data).toBe('m0');
  expect(Array.from(new Uint8Array(received[1].data as ArrayBuffer))).toEqual([1]);
  expect(received[2].data).toBe('m2');
});

test('messages before onmessage is assigned are buffered in order', () => {
  const ws = new MonetWebSocket('ws://test/burst');
  native.onOpen!({});
  native.queue.push(bytes(7), text('late'), bytes(9));
  native.onMessage!({isBinary: true});
  native.onMessage!({isBinary: false});
  native.onMessage!({isBinary: true});

  const received: {data: string | ArrayBuffer; isBinary: boolean}[] = [];
  ws.onmessage = event => received.push(event);

  expect(received.map(event => event.isBinary)).toEqual([true, false, true]);
  expect(Array.from(new Uint8Array(received[0].data as ArrayBuffer))).toEqual([7]);
  expect(received[1].data).toBe('late');
  expect(Array.from(new Uint8Array(received[2].data as ArrayBuffer))).toEqual([9]);
});
