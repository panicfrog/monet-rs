// The Response half of the WHATWG-fetch-compatible layer: the head
// snapshot plus a body channel that mirrors the native push queue. The
// protocol below is the JS mirror of MonetModule's fetch channel (see
// packages/monet-rn/src/lib.rs): the pump pushes chunks natively and
// signals via on_chunk; JS drains them with zero-copy take_chunk pulls.

import type { Monet } from '../generated/rust-turbo/Monet';
import { utf8Decode } from './utf8';

/** One head event as delivered by `on_head`. */
export interface HeadEvent {
  status: number;
  headers: Array<{name: string; value: string}>;
}

/** One error event as delivered by `on_error`; `kind` mirrors the core
 * error discriminant (Cancelled | Hook | Transport). */
export interface ErrorEvent {
  kind: string;
  message: string;
}

/** The error surface of the fetch channel. */
export class MonetError extends Error {
  readonly kind: string;

  constructor(kind: string, message: string) {
    super(message);
    this.name = 'MonetError';
    this.kind = kind;
  }
}

/** A minimal case-insensitive header view (WHATWG Headers subset). */
export class MonetHeaders {
  private readonly raw: Array<{name: string; value: string}>;

  constructor(raw: Array<{name: string; value: string}>) {
    this.raw = raw;
  }

  get(name: string): string | null {
    const found = this.raw.find(
      entry => entry.name.toLowerCase() === name.toLowerCase(),
    );
    return found ? found.value : null;
  }

  has(name: string): boolean {
    return this.get(name) !== null;
  }

  *entries(): IterableIterator<[string, string]> {
    for (const entry of this.raw) {
      yield [entry.name, entry.value];
    }
  }
}

/**
 * The native push queue mirrored on the JS side. The fetch() wiring feeds
 * it (on_chunk drains take_chunk into `chunks`); the body iterator
 * consumes it. Everything runs on the JS thread, so plain fields suffice.
 */
export class BodyChannel {
  private readonly queued: Uint8Array[] = [];
  private done = false;
  private failure: MonetError | null = null;
  private waiters: Array<() => void> = [];

  push(chunk: Uint8Array): void {
    this.queued.push(chunk);
    this.wake();
  }

  finish(): void {
    this.done = true;
    this.wake();
  }

  fail(error: MonetError): void {
    this.failure = error;
    this.wake();
  }

  take(): Uint8Array | undefined {
    return this.queued.shift();
  }

  get error(): MonetError | null {
    return this.failure;
  }

  get isDone(): boolean {
    return this.done;
  }

  /** Resolves on the next signal or terminal state. */
  wait(): Promise<void> {
    if (this.queued.length > 0 || this.done || this.failure) {
      return Promise.resolve();
    }
    return new Promise<void>(resolve => {
      this.waiters.push(resolve);
    });
  }

  private wake(): void {
    const pending = this.waiters;
    this.waiters = [];
    for (const resolve of pending) {
      resolve();
    }
  }
}

/** Concatenates chunks into one ArrayBuffer (arrayBuffer()'s body form). */
function concat(parts: Uint8Array[]): ArrayBuffer {
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out.buffer;
}

/**
 * The fetch Response: head fields up front, the body as an async
 * iterable of raw chunks. Drain fully for arrayBuffer/text/json; breaking
 * out of the iteration early aborts the transfer and releases the native
 * fetch state (see bodyIterator's finally).
 */
export class MonetResponse {
  readonly status: number;
  readonly ok: boolean;
  readonly headers: MonetHeaders;
  readonly body: AsyncIterableIterator<Uint8Array>;

  constructor(
    fetchId: number,
    monet: Monet,
    head: HeadEvent,
    channel: BodyChannel,
  ) {
    this.status = head.status;
    this.ok = head.status >= 200 && head.status < 300;
    this.headers = new MonetHeaders(head.headers ?? []);
    this.body = bodyIterator(fetchId, monet, channel);
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
    const parts: Uint8Array[] = [];
    for await (const chunk of this.body) {
      parts.push(chunk);
    }
    return concat(parts);
  }

  async text(): Promise<string> {
    return utf8Decode(new Uint8Array(await this.arrayBuffer()));
  }

  async json(): Promise<unknown> {
    return JSON.parse(await this.text());
  }
}

async function* bodyIterator(
  fetchId: number,
  monet: Monet,
  channel: BodyChannel,
): AsyncGenerator<Uint8Array, void, unknown> {
  try {
    for (;;) {
      let chunk = channel.take();
      while (chunk !== undefined) {
        yield chunk;
        chunk = channel.take();
      }
      const error = channel.error;
      if (error) {
        throw error;
      }
      if (channel.isDone) {
        return;
      }
      await channel.wait();
    }
  } finally {
    // Drained, thrown, or abandoned early — either way the transfer is
    // over for this consumer. abort() stops a still-running pump (a
    // no-op once terminated); cleanupFetch releases the native state.
    // Both are safe in any state, exactly once is enough.
    monet.abort(fetchId);
    monet.cleanupFetch(fetchId);
  }
}
