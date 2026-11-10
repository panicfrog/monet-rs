// The fetch entry point: WHATWG-fetch-compatible subset over the Monet
// TurboModule's callback-signal + zero-copy-pull channel.
//
//   fetch(url, init) → Promise<MonetResponse>
//
// init: { method, headers, body (string | ArrayBuffer |
//         AsyncIterable<string | ArrayBuffer>), signal (AbortSignal),
//         timeoutMs, followRedirects }. A streaming body rides a channel
//         the producer feeds chunk by chunk (fetchStream + friends); a
//         streamed body cannot be retried by a response hook — the fetch
//         fails instead of silently re-sending an empty body.

import { getMonet } from '../generated/rust-turbo/Monet';
import type { FetchOptions } from '../generated/rust-turbo/structs/FetchOptions';
import {
  BodyChannel,
  HeadEvent,
  MonetError,
  MonetResponse,
} from './response';
import { utf8Encode } from './utf8';

export interface MonetRequestInit {
  method?: string;
  headers?:
    | Record<string, string>
    | ReadonlyArray<readonly [string, string]>
    | ReadonlyArray<{name: string; value: string}>;
  /** Buffered (string | ArrayBuffer) or streamed (AsyncIterable of
   *  either) — the streaming form feeds the wire chunk by chunk. */
  body?: string | ArrayBuffer | AsyncIterable<string | ArrayBuffer>;
  /** Client-side metadata riding the hook/observer events (the flow tag
   *  that strings a series of requests together; never on the wire). */
  metadata?: Record<string, string>;
  signal?: AbortSignal;
  /** Recorded for forward compatibility (core-level knob lands later). */
  timeoutMs?: number;
  followRedirects?: boolean;
}

type HeaderKV = {name: string; value: string};

function normalizeHeaders(
  init: MonetRequestInit,
): HeaderKV[] {
  const out: HeaderKV[] = [];
  const source = init.headers;
  if (!source) {
    return out;
  }
  if (Array.isArray(source)) {
    for (const entry of source) {
      out.push(
        Array.isArray(entry)
          ? {name: entry[0], value: entry[1]}
          : {name: entry.name, value: entry.value},
      );
    }
  } else {
    // Array.isArray's false branch does not narrow ReadonlyArray unions
    // away (TS quirk); the record shape is the only remaining option.
    const record = source as Record<string, string>;
    for (const name of Object.keys(record)) {
      out.push({name, value: record[name]});
    }
  }
  return out;
}

/** The init's metadata bag → the options' KV pairs ({} when absent). */
function metadataKV(
  init: MonetRequestInit,
): Array<{name: string; value: string}> {
  const out: Array<{name: string; value: string}> = [];
  if (init.metadata) {
    for (const [name, value] of Object.entries(init.metadata)) {
      out.push({name, value});
    }
  }
  return out;
}

export function fetch(
  input: string | {url: string},
  init: MonetRequestInit = {},
): Promise<MonetResponse> {
  const monet = getMonet();
  const url = typeof input === 'string' ? input : input.url;
  const options: FetchOptions = {
    url,
    method: init.method ?? 'GET',
    headers: normalizeHeaders(init),
    metadata: metadataKV(init),
    timeoutMs: init.timeoutMs ?? 0,
    followRedirects: init.followRedirects ?? true,
  };
  let body: ArrayBuffer | null = null;
  if (typeof init.body === 'string') {
    body = utf8Encode(init.body);
  } else if (
    init.body != null &&
    typeof (init.body as ArrayBuffer).byteLength === 'number'
  ) {
    body = init.body as ArrayBuffer;
  }
  const streamingBody: AsyncIterable<string | ArrayBuffer> | null =
    init.body != null && body == null
      ? (init.body as AsyncIterable<string | ArrayBuffer>)
      : null;

  const channel = new BodyChannel();
  let headResolved = false;

  return new Promise<MonetResponse>((resolve, reject) => {
    // The pump's callbacks are marshalled back to this (JS) thread, so
    // they cannot run before this executor returns — wiring the abort
    // listener after fetch() is race-free.
    const wireCallbacks = (
      head: (event: HeadEvent) => void,
      chunk: () => void,
      done: () => void,
      error: (event: {kind: string; message: string}) => void,
    ): number =>
      streamingBody != null
        ? monet.fetchStream(options, head, chunk, done, error)
        : monet.fetch(options, body, head, chunk, done, error);
    const fetchId = wireCallbacks(
      (head: HeadEvent) => {
        headResolved = true;
        // NOTE: the abort listener stays attached — a signal aborts the
        // whole fetch, body included, not just the head phase.
        resolve(new MonetResponse(fetchId, monet, head, channel));
      },
      () => {
        // A chunk-signalled pull: drain everything the pump queued.
        for (;;) {
          const chunk = monet.takeChunk(fetchId);
          if (chunk == null) {
            break;
          }
          channel.push(new Uint8Array(chunk));
        }
      },
      () => {
        detachAbort();
        channel.finish();
      },
      (event) => {
        detachAbort();
        const error = new MonetError(
          event?.kind ?? 'Transport',
          event?.message ?? 'unknown transport error',
        );
        if (!headResolved) {
          reject(error);
        }
        channel.fail(error);
      },
    );

    if (streamingBody != null) {
      // The producer loop: chunks cross the bridge as they arrive; the
      // stream ends with complete, a producer error aborts the upload.
      (async () => {
        for await (const chunk of streamingBody) {
          const bytes =
            typeof chunk === 'string' ? utf8Encode(chunk) : chunk;
          monet.fetchStreamChunk(fetchId, bytes);
        }
        monet.fetchStreamComplete(fetchId);
      })().catch(producerError => {
        monet.fetchStreamError(fetchId, String(producerError));
      });
    }

    const onAbort = () => {
      monet.abort(fetchId);
    };
    const detachAbort = () => {
      init.signal?.removeEventListener('abort', onAbort);
    };
    if (init.signal) {
      if (init.signal.aborted) {
        onAbort();
      } else {
        init.signal.addEventListener('abort', onAbort);
      }
    }
  });
}
