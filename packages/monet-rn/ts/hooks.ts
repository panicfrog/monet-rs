// The hook/observer layer: TS wrappers over the Monet TurboModule's hook
// channel (see docs/plans/2026-11-hooks.md).
//
// Two registrations, strictly separated:
// - **Observers** are read-only: the callback sees the final form (after
//   every hook) and returns nothing — the pipeline never waits. Business
//   code can add these freely (logging, metrics, tracing).
// - **Hooks** are read-write: the callback must return the modified
//   value (or throw), the pipeline waits for it, and a response hook can
//   ask for a retry by returning `{retry: true}`. These deserve review:
//   a hook that never resolves hangs its fetch until aborted.
//
// The resolve dance is entirely in the wrappers below — user code writes
// plain (possibly async) functions and never sees hookCallId.

import { getMonet } from '../generated/rust-turbo/Monet';

/** A request as hooks see and rewrite it (the body passes through
 *  untouched — v1 keeps bodies out of the protocol). */
export interface MonetHookRequest {
  url: string;
  method: string;
  headers: Array<{name: string; value: string}>;
}

/** A response head as hooks see and rewrite it. */
export interface MonetHookResponse {
  status: number;
  headers: Array<{name: string; value: string}>;
}

/** The retry signal a response hook returns instead of a response. */
export interface MonetHookRetry {
  retry: true;
}

/**
 * A mocked response a request hook can return instead of the request
 * (v2/W3): the hook answered the fetch itself — no transport runs,
 * nothing goes on the wire, and the mocked response still passes
 * through the response hooks (and their rewrites) before delivery.
 * The body is UTF-8 text (JSON/text fixtures; binary mocks are not in
 * the protocol yet).
 */
export interface MonetMockedResponse {
  mocked: {
    status: number;
    headers: Array<{name: string; value: string}>;
    body?: string;
  };
}

/**
 * The call-site metadata bag (`fetch(url, {metadata})`): the flow tag
 * that strings a series of requests together. Client-side only — never
 * on the wire; send it as a header too when the server needs it.
 */
export type MonetHookMetadata = Record<string, string>;

/** What every hook/observer callback receives next to the value: the
 *  fetch's pairing key (stable across retries), the attempt number, and
 *  the call-site metadata. */
export interface MonetHookContext {
  correlationId: number;
  attempt: number;
  metadata: MonetHookMetadata;
}

/** The event the native side delivers: the value plus the call context. */
interface HookRequestEvent {
  correlationId: number;
  hookCallId: number;
  attempt: number;
  metadata: Array<{name: string; value: string}>;
  url: string;
  method: string;
  headers: Array<{name: string; value: string}>;
}

interface HookResponseEvent {
  correlationId: number;
  hookCallId: number;
  attempt: number;
  metadata: Array<{name: string; value: string}>;
  status: number;
  headers: Array<{name: string; value: string}>;
}

type HeaderKV = {name: string; value: string};

/** Extracts the request view from a native event. */
function toRequest(event: HookRequestEvent): MonetHookRequest {
  return {url: event.url, method: event.method, headers: event.headers};
}

/** Extracts the response view from a native event. */
function toResponse(event: HookResponseEvent): MonetHookResponse {
  return {status: event.status, headers: event.headers};
}

/** Extracts the call context from a native event. */
function toContext(
  event: HookRequestEvent | HookResponseEvent,
): MonetHookContext {
  const metadata: MonetHookMetadata = {};
  for (const pair of event.metadata) {
    metadata[pair.name] = pair.value;
  }
  return {
    correlationId: event.correlationId,
    attempt: event.attempt,
    metadata,
  };
}

/**
 * Registers a read-only request observer. The callback sees the final
 * request — after every mutating hook, right before it goes on the wire
 * — and cannot affect it: no return value, the pipeline does not wait.
 * The context carries the fetch's correlationId (pairs with the response
 * events), the attempt, and the call-site metadata (the flow tag).
 * Use freely for logging, tracing, and metrics.
 */
export function addRequestObserver(
  observer: (req: MonetHookRequest, ctx: MonetHookContext) => void,
): void {
  getMonet().addRequestObserver((event: HookRequestEvent) => {
    observer(toRequest(event), toContext(event));
  });
}

/**
 * Registers a read-only response observer. The callback sees the final
 * response head — after every mutating hook, before it is delivered —
 * with the same cannot-affect, never-waits contract as
 * {@link addRequestObserver}. The context's correlationId matches the
 * request events of the same fetch.
 */
export function addResponseObserver(
  observer: (res: MonetHookResponse, ctx: MonetHookContext) => void,
): void {
  getMonet().addResponseObserver((event: HookResponseEvent) => {
    observer(toResponse(event), toContext(event));
  });
}

/**
 * Registers a read-write request hook. The callback may rewrite the
 * request (url/method/headers) and must return it, or answer the fetch
 * itself by returning a {@link MonetMockedResponse} — no transport
 * runs, nothing goes on the wire, and the mock still passes through
 * the response hooks. Throwing (or rejecting) fails the fetch with a
 * `Hook` error. The pipeline waits for the resolution — a hook that
 * never resolves hangs its fetch until aborted, so keep the async work
 * bounded.
 *
 * Runs on every fetch and every WebSocket connect (the upgrade request),
 * in registration order together with any other request hooks. The
 * context carries correlationId/attempt/metadata (read-only — the
 * metadata bag comes from the call site, not the hook).
 */
export function addRequestHook(
  hook: (
    req: MonetHookRequest,
    ctx: MonetHookContext,
  ) =>
    | MonetHookRequest
    | MonetMockedResponse
    | Promise<MonetHookRequest | MonetMockedResponse>,
): void {
  getMonet().addRequestHook(async (event: HookRequestEvent) => {
    try {
      const out = await hook(toRequest(event), toContext(event));
      if (out !== null && typeof out === 'object' && 'mocked' in out) {
        // The hook answered the fetch itself: the mocked response rides
        // the resolution; url/method/headers pass the event's values
        // through (the rewrite fields are unused on this arm).
        getMonet().resolveRequestHook(event.hookCallId, {
          url: event.url,
          method: event.method,
          headers: event.headers,
          mocked: {
            status: out.mocked.status,
            headers: out.mocked.headers,
            body: out.mocked.body,
          },
        });
        return;
      }
      const modified = out;
      getMonet().resolveRequestHook(event.hookCallId, {
        url: modified.url,
        method: modified.method,
        headers: modified.headers,
      });
    } catch (error) {
      getMonet().rejectRequestHook(event.hookCallId, {
        kind: error instanceof Error ? error.name : 'Hook',
        message: error instanceof Error ? error.message : String(error),
      });
    }
  });
}

/**
 * Registers a read-write response hook. The callback sees the response
 * head before the fetch delivers it, and may:
 * - return the (possibly rewritten) response → the fetch proceeds,
 * - return `{retry: true}` → the whole pipeline re-runs from the
 *   original request (request hooks included — they can inject a fresh
 *   token), up to the module's hard cap,
 * - throw → the fetch fails with a `Hook` error.
 *
 * The context's `attempt` (0 on the first run) lets a hook give up
 * after its own bound instead of leaning on the cap; `correlationId`
 * pairs with the request events of the same fetch, `metadata` carries
 * the call-site flow tag.
 */
export function addResponseHook(
  hook: (
    res: MonetHookResponse,
    ctx: MonetHookContext,
  ) =>
    | MonetHookResponse
    | MonetHookRetry
    | Promise<MonetHookResponse | MonetHookRetry>,
): void {
  getMonet().addResponseHook(async (event: HookResponseEvent) => {
    try {
      const outcome = await hook(toResponse(event), toContext(event));
      if ('retry' in outcome) {
        getMonet().retryResponseHook(event.hookCallId);
        return;
      }
      getMonet().resolveResponseHook(event.hookCallId, {
        status: outcome.status,
        headers: outcome.headers,
      });
    } catch (error) {
      getMonet().rejectResponseHook(event.hookCallId, {
        kind: error instanceof Error ? error.name : 'Hook',
        message: error instanceof Error ? error.message : String(error),
      });
    }
  });
}

/**
 * Drops every hook and observer registration (the `clearPins`
 * symmetry), and every outstanding hook call. Pending hooks wake with an
 * error.
 */
export function clearHooks(): void {
  getMonet().clearHooks();
}
