// The fetch entry point: WHATWG-fetch-compatible subset over the Monet
// TurboModule's callback-signal + zero-copy-pull channel.
//
//   fetch(url, init) → Promise<MonetResponse>
//
// init: { method, headers, body (string | ArrayBuffer | Blob |
//         MonetFormData | AsyncIterable<string | ArrayBuffer>), signal
//         (AbortSignal), timeoutMs, followRedirects }. A streaming body
//         rides a channel the producer feeds chunk by chunk (fetchStream
//         + friends); a streamed body cannot be retried by a response
//         hook — the fetch fails instead of silently re-sending an empty
//         body.

import { getMonet } from '../generated/rust-turbo/Monet';
import type { FetchOptions } from '../generated/rust-turbo/structs/FetchOptions';
import {
  BodyChannel,
  HeadEvent,
  MonetError,
  MonetResponse,
} from './response';
import {blobBodyStream, isBlob, BlobLike} from './blob';
import {MonetFormData} from './formdata';
import { utf8Encode } from './utf8';

export interface MonetRequestInit {
  method?: string;
  headers?:
    | Record<string, string>
    | ReadonlyArray<readonly [string, string]>
    | ReadonlyArray<{name: string; value: string}>;
  /** Buffered (string | ArrayBuffer | MonetFormData) or streamed
   *  (AsyncIterable of string | ArrayBuffer) — the streaming form feeds
   *  the wire chunk by chunk. A MonetFormData serializes to its
   *  multipart body and sets the content-type (an explicit content-type
   *  header wins, as with browsers). A Blob rides the streaming route
   *  (FileReader at pump time; the Blob's type sets the content-type
   *  when none is explicit). */
  body?:
    | string
    | ArrayBuffer
    | BlobLike
    | MonetFormData
    | AsyncIterable<string | ArrayBuffer>;
  /** Client-side metadata riding the hook/observer events (the flow tag
   *  that strings a series of requests together; never on the wire). */
  metadata?: Record<string, string>;
  signal?: AbortSignal;
  /** Recorded for forward compatibility (core-level knob lands later). */
  timeoutMs?: number;
  followRedirects?: boolean;
  /** The prefetch cache key (v2/W4): a fetch carrying a key that
   *  matches a fresh prefetch() entry delivers it with zero transport;
   *  a pending one joins it. Never goes on the wire. */
  prefetchKey?: string;
  /** The freshness window for cache reads, milliseconds (0 = the 5s
   *  default, nitro's prefetchCacheTtlMs). Evaluated when this fetch
   *  reads the cache. */
  prefetchTtlMs?: number;
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
  const headers = normalizeHeaders(init);
  let body: ArrayBuffer | null = null;
  let streamingForm: AsyncGenerator<string | ArrayBuffer> | null = null;
  let formdataContentType: string | null = null;
  let blobStream: AsyncGenerator<ArrayBuffer> | null = null;
  let blobType: string | null = null;
  if (typeof init.body === 'string') {
    body = utf8Encode(init.body);
  } else if (init.body instanceof MonetFormData) {
    // The FormData's content-type rides the headers when the caller did
    // not set one explicitly. A form with file/blob parts streams (the
    // native reader or the FileReader yields the bytes; a large file
    // never crosses into JS memory whole); a pure form serializes
    // inline.
    formdataContentType = init.body.contentType();
    if (init.body.hasFileParts()) {
      streamingForm = init.body.stream();
    } else {
      body = init.body.serialize().body;
    }
    const hasContentType = headers.some(
      header => header.name.toLowerCase() === 'content-type',
    );
    if (!hasContentType && formdataContentType !== null) {
      headers.push({name: 'content-type', value: formdataContentType});
    }
  } else if (isBlob(init.body)) {
    // A Blob body rides the streaming route too: the FileReader read
    // is async and fetch itself stays synchronous. The Blob's type
    // rides the headers when the caller did not set one (the browser
    // behavior).
    blobStream = blobBodyStream(init.body);
    blobType = init.body.type || null;
    if (blobType && !headers.some(h => h.name.toLowerCase() === 'content-type')) {
      headers.push({name: 'content-type', value: blobType});
    }
  } else if (
    init.body != null &&
    typeof (init.body as ArrayBuffer).byteLength === 'number'
  ) {
    body = init.body as ArrayBuffer;
  }
  const streamingBody: AsyncIterable<string | ArrayBuffer> | null =
    streamingForm ??
    blobStream ??
    (init.body != null && body == null
      ? (init.body as AsyncIterable<string | ArrayBuffer>)
      : null);
  const options: FetchOptions = {
    url,
    method: init.method ?? 'GET',
    headers,
    metadata: metadataKV(init),
    timeoutMs: init.timeoutMs ?? 0,
    followRedirects: init.followRedirects ?? true,
    prefetchKey: init.prefetchKey ?? '',
    prefetchTtlMs: init.prefetchTtlMs ?? 0,
  };

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

/**
 * Runs a real fetch now and stores its complete response under the
 * prefetch key — a later `fetch(url, {prefetchKey})` delivers it with
 * zero transport while fresh (default 5s), or joins this one if still
 * in flight. Racing prefetches for one key start a single request.
 * The request-hook chain runs (auth injection rides it); response
 * hooks do not (the response goes to the cache, not a consumer).
 */
export function prefetch(
  input: string | {url: string},
  init: MonetRequestInit = {},
): Promise<void> {
  const monet = getMonet();
  const url = typeof input === 'string' ? input : input.url;
  const options: FetchOptions = {
    url,
    method: init.method ?? 'GET',
    headers: normalizeHeaders(init),
    metadata: metadataKV(init),
    timeoutMs: init.timeoutMs ?? 0,
    followRedirects: init.followRedirects ?? true,
    prefetchKey: init.prefetchKey ?? '',
    prefetchTtlMs: init.prefetchTtlMs ?? 0,
  };
  return new Promise<void>((resolve, reject) => {
    monet.prefetch(
      options,
      () => resolve(),
      (event) =>
        reject(
          new MonetError(
            event?.kind ?? 'Transport',
            event?.message ?? 'prefetch failed',
          ),
        ),
    );
  });
}
