// monet-rn's public surface: the WHATWG-fetch-compatible layer over the
// Rust TurboModule. The generated module bindings (generated/rust-turbo/)
// are re-exported so consumers never import them directly — the generated
// tree appears on the first native build and is otherwise an
// implementation detail.

export { getMonet } from '../generated/rust-turbo/Monet';
export type { Monet } from '../generated/rust-turbo/Monet';

export { fetch, prefetch } from './fetch';
export type { MonetRequestInit } from './fetch';
export {
  addRequestHook,
  addRequestObserver,
  addResponseHook,
  addResponseObserver,
  clearHooks,
} from './hooks';
export type {
  MonetHookContext,
  MonetHookMetadata,
  MonetHookRequest,
  MonetHookResponse,
  MonetHookRetry,
  MonetMockedResponse,
} from './hooks';
export {
  addExchangeRecorder,
  type MonetExchange,
  type MonetExchangeBody,
} from './exchange';
export { MonetFormData } from './formdata';
export { MonetError, MonetHeaders, MonetResponse } from './response';
export type { ErrorEvent, HeadEvent } from './response';
export { MonetWebSocket } from './websocket';
export type {
  MonetWsCloseEvent,
  MonetWsErrorEvent,
  MonetWsMessageEvent,
} from './websocket';

import { getMonet } from '../generated/rust-turbo/Monet';

/**
 * Installs a pinning configuration (nitro-fetch-pinning.json format,
 * fail-closed: a malformed config throws, never a silent no-op).
 */
export function configurePins(json: string): void {
  getMonet().configurePins(json);
}

/** Clears the pinning configuration (rebuilds the transport/client). */
export function clearPins(): void {
  getMonet().clearPins();
}

/** Drops every cached prefetch entry (fresh and pending alike). */
export function clearPrefetch(): void {
  getMonet().clearPrefetch();
}

/**
 * Installs the built-in retry policy: `{"retries": n, "backoffMs": ms}`
 * — n extra attempts on a 5xx response or a network error, the backoff
 * doubling per attempt. Off by default; coexists with the hook retry
 * exit. The last response is the fetch's result when the cap runs out.
 */
export function configureRetry(policy: {
  retries: number;
  backoffMs: number;
}): void {
  getMonet().configureRetry(JSON.stringify(policy));
}

/** Clears the built-in retry policy (back to off). */
export function clearRetry(): void {
  getMonet().clearRetry();
}

/**
 * Sets the app-supplied trust roots (each a PEM block or bare base64
 * DER) anchored alongside the platform trust store —
 * network_security_config semantics (system CAs stay trusted).
 * Fail-closed: an entry that does not decode throws and installs
 * nothing.
 *
 * Reach: WebSocket connections and (on Android < 34) the rustls fetch
 * fallback. The platform HttpEngine fetch path has no custom-roots
 * API — those fetches trust the system + network_security_config
 * anchors only.
 */
export function configureTrust(roots: string[]): void {
  getMonet().configureTrust(roots);
}

/** Clears the custom trust roots (rebuilds the transport/client). */
export function clearTrust(): void {
  getMonet().clearTrust();
}
