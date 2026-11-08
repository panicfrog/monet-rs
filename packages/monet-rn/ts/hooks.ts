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

/** The event the native side delivers: the value plus the call context. */
interface HookRequestEvent {
  hookCallId: number;
  attempt: number;
  url: string;
  method: string;
  headers: Array<{name: string; value: string}>;
}

interface HookResponseEvent {
  hookCallId: number;
  attempt: number;
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

/**
 * Registers a read-only request observer. The callback sees the final
 * request — after every mutating hook, right before it goes on the wire
 * — and cannot affect it: no return value, the pipeline does not wait.
 * Use freely for logging, tracing, and metrics.
 */
export function addRequestObserver(
  observer: (req: MonetHookRequest) => void,
): void {
  getMonet().addRequestObserver((event: HookRequestEvent) => {
    observer(toRequest(event));
  });
}

/**
 * Registers a read-only response observer. The callback sees the final
 * response head — after every mutating hook, before it is delivered —
 * with the same cannot-affect, never-waits contract as
 * {@link addRequestObserver}.
 */
export function addResponseObserver(
  observer: (res: MonetHookResponse) => void,
): void {
  getMonet().addResponseObserver((event: HookResponseEvent) => {
    observer(toResponse(event));
  });
}

/**
 * Registers a read-write request hook. The callback may rewrite the
 * request (url/method/headers) and must return it; throwing (or
 * rejecting) fails the fetch with a `Hook` error. The pipeline waits for
 * the resolution — a hook that never resolves hangs its fetch until
 * aborted, so keep the async work bounded.
 *
 * Runs on every fetch and every WebSocket connect (the upgrade request),
 * in registration order together with any other request hooks.
 */
export function addRequestHook(
  hook: (req: MonetHookRequest) => MonetHookRequest | Promise<MonetHookRequest>,
): void {
  getMonet().addRequestHook(async (event: HookRequestEvent) => {
    try {
      const modified = await hook(toRequest(event));
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
 * The second argument is the attempt number (0 on the first run), so a
 * hook can give up after its own bound instead of leaning on the cap.
 */
export function addResponseHook(
  hook: (
    res: MonetHookResponse,
    attempt: number,
  ) => MonetHookResponse | MonetHookRetry | Promise<MonetHookResponse | MonetHookRetry>,
): void {
  getMonet().addResponseHook(async (event: HookResponseEvent) => {
    try {
      const outcome = await hook(toResponse(event), event.attempt);
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
