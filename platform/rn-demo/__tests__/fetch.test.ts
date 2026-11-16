/**
 * fetch()'s native-state lifetime, against a fake native module.
 *
 * Each fetch owns a registry entry natively until JS calls cleanupFetch.
 * It must be released once the transfer terminates — including when the
 * fetch fails before its head, and when the caller never reads the body.
 */

import {fetch} from '../../../packages/monet-rn/ts/fetch';

type Signal = (event?: any) => void;

interface FakeFetch {
  onHead: Signal;
  onChunk: Signal;
  onDone: Signal;
  onError: Signal;
  queue: ArrayBuffer[];
}

const native = {
  nextId: 0,
  fetches: new Map<number, FakeFetch>(),
  cleaned: [] as number[],
};

// getMonet() reads the binding the native side installs on
// globalThis.RustTurbo; install a fake Monet there instead.
const fakeMonet = {
  fetch: (
    _options: unknown,
    _body: ArrayBuffer | null,
    onHead: Signal,
    onChunk: Signal,
    onDone: Signal,
    onError: Signal,
  ) => {
    const id = ++native.nextId;
    native.fetches.set(id, {onHead, onChunk, onDone, onError, queue: []});
    return id;
  },
  takeChunk: (id: number) => native.fetches.get(id)?.queue.shift() ?? null,
  abort: () => true,
  cleanupFetch: (id: number) => {
    native.cleaned.push(id);
  },
};
(globalThis as any).RustTurbo = {get: () => fakeMonet};

beforeEach(() => {
  native.fetches.clear();
  native.cleaned = [];
});

test('a fetch that fails before its head releases the native state', async () => {
  const pending = fetch('http://test/down');
  const id = native.nextId;
  native.fetches.get(id)!.onError({kind: 'Transport', message: 'refused'});

  await expect(pending).rejects.toThrow('refused');
  expect(native.cleaned).toContain(id);
});

test('a response whose body is never read releases the native state', async () => {
  const pending = fetch('http://test/unread');
  const id = native.nextId;
  const wire = native.fetches.get(id)!;
  wire.onHead({status: 500, headers: []});
  const response = await pending;
  expect(response.ok).toBe(false);

  // The caller checks `ok` and walks away; the transfer still ends.
  wire.queue.push(new Uint8Array([1, 2]).buffer);
  wire.onChunk();
  wire.onDone();

  expect(native.cleaned).toContain(id);
});

test('a body that errors after the head releases the native state', async () => {
  const pending = fetch('http://test/reset');
  const id = native.nextId;
  const wire = native.fetches.get(id)!;
  wire.onHead({status: 200, headers: []});
  await pending;
  wire.onError({kind: 'Transport', message: 'reset'});

  expect(native.cleaned).toContain(id);
});

test('the body still reads in full after the state is released', async () => {
  const pending = fetch('http://test/body');
  const id = native.nextId;
  const wire = native.fetches.get(id)!;
  wire.onHead({status: 200, headers: []});
  const response = await pending;
  wire.queue.push(new Uint8Array([104, 105]).buffer);
  wire.onChunk();
  wire.onDone();

  expect(await response.text()).toBe('hi');
});
