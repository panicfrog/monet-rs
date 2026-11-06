// monet-rn's public surface: the WHATWG-fetch-compatible layer over the
// Rust TurboModule. The generated module bindings (generated/rust-turbo/)
// are re-exported so consumers never import them directly — the generated
// tree appears on the first native build and is otherwise an
// implementation detail.

export { getMonet } from '../generated/rust-turbo/Monet';
export type { Monet } from '../generated/rust-turbo/Monet';

export { fetch } from './fetch';
export type { MonetRequestInit } from './fetch';
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
