// The exchange recorder (v2/W3b): one complete record per fetch, fired
// at its terminal. The nitro-fetch NetworkInspector's record shape, fed
// from the native pump so every field is the final form (post-hook
// request, the head the pump delivered, bodies capped to a text prefix).

import { getMonet } from '../generated/rust-turbo/Monet';

/** A body as the record reports it: a UTF-8 text prefix (null for
 *  binary content — the size still tells the truth) plus the full byte
 *  count. The prefix is capped at 64KB natively. */
export interface MonetExchangeBody {
  text: string | null;
  size: number;
}

/** One fetch's complete record: the final request form (post-hook),
 *  the response as delivered, both bodies (buffered requests only — a
 *  streamed upload has no complete form at any single point), the
 *  duration, and the terminal error when the fetch failed. */
export interface MonetExchange {
  correlationId: number;
  url: string;
  method: string;
  requestHeaders: Array<{name: string; value: string}>;
  requestBody: MonetExchangeBody | null;
  status: number;
  responseHeaders: Array<{name: string; value: string}>;
  responseBody: MonetExchangeBody | null;
  durationMs: number;
  error: string | null;
}

/**
 * Registers an exchange recorder. Fire-and-forget like the observers;
 * one record per fetch at its terminal (completion or failure — a
 * failed fetch records the error and whatever was captured).
 * `clearHooks` drops these registrations too.
 */
export function addExchangeRecorder(
  recorder: (exchange: MonetExchange) => void,
): void {
  getMonet().addExchangeRecorder((event: MonetExchange) => {
    recorder(event);
  });
}
