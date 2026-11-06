// MonetWebSocket: the EventTarget-style WebSocket over the Rust
// TurboModule's ws_* channel (callback-signal + zero-copy-pull — the
// same protocol as the fetch layer). The class wiring lives entirely
// here; the native side only fires callbacks.

import { getMonet } from '../generated/rust-turbo/Monet';

/** One message event: the data (text or binary) and its type. */
export interface MonetWsMessageEvent {
  data: string | ArrayBuffer;
  isBinary: boolean;
}

/** One close event — code and reason round-trip exactly. */
export interface MonetWsCloseEvent {
  code: number;
  reason: string;
  wasClean: boolean;
}

/** One error event. */
export interface MonetWsErrorEvent {
  kind: string;
  message: string;
}

type MessageHandler = (event: MonetWsMessageEvent) => void;
type CloseHandler = (event: MonetWsCloseEvent) => void;
type ErrorHandler = (event: MonetWsErrorEvent) => void;
type OpenHandler = () => void;

/**
 * A WebSocket connection. WHATWG WebSocket-compatible surface (open
 * fires on handshake, message/close/error as events; send/close/ping as
 * methods) over the native channel. The `close` event's code and reason
 * round-trip exactly — the fidelity that motivated the iOS transport's
 * hand-rolled framing.
 */
export class MonetWebSocket {
  /** The native connection id (0 until ws_connect returns). */
  private connId: number = -1;
  private _readyState: number = 0; // CONNECTING

  // Event handlers (EventTarget-style property assignment).
  private _onopen: OpenHandler | null = null;
  private _onmessage: MessageHandler | null = null;
  private _onclose: CloseHandler | null = null;
  private _onerror: ErrorHandler | null = null;

  /** 'blob' is accepted for compatibility but always delivers ArrayBuffer. */
  binaryType: 'arraybuffer' | 'blob' = 'arraybuffer';

  // Messages that arrived before onmessage was assigned (the native
  // connect fires callbacks immediately — the constructor can't wait
  // for the caller to wire handlers first).
  private messageBuffer: MonetWsMessageEvent[] = [];

  // Close/error events that arrived before their handlers were assigned.
  private closeBuffer: MonetWsCloseEvent | null = null;
  private _closed = false;

  // The isBinary flags from on_message signals, paired FIFO with
  // ws_take_message pulls.
  private pendingFlags: boolean[] = [];

  get onopen(): OpenHandler | null {
    return this._onopen;
  }
  set onopen(handler: OpenHandler | null) {
    this._onopen = handler;
  }

  get onmessage(): MessageHandler | null {
    return this._onmessage;
  }
  set onmessage(handler: MessageHandler | null) {
    this._onmessage = handler;
    if (handler !== null && this.messageBuffer.length > 0) {
      const buffered = this.messageBuffer;
      this.messageBuffer = [];
      for (const event of buffered) {
        handler(event);
      }
    }
  }

  get onclose(): CloseHandler | null {
    return this._onclose;
  }
  set onclose(handler: CloseHandler | null) {
    this._onclose = handler;
    if (handler !== null && this.closeBuffer !== null) {
      const buffered = this.closeBuffer;
      this.closeBuffer = null;
      handler(buffered);
    }
  }

  get onerror(): ErrorHandler | null {
    return this._onerror;
  }
  set onerror(handler: ErrorHandler | null) {
    this._onerror = handler;
  }

  /**
   * Connects to `url` (ws:// or wss://). The connection starts
   * immediately; `onopen` fires when the handshake completes.
   */
  constructor(
    url: string,
    options?: {
      protocols?: string[];
      headers?: Record<string, string>;
    },
  ) {
    this._url = url;
    const monet = getMonet();
    const headers: {name: string; value: string}[] = [];
    if (options?.headers) {
      for (const [name, value] of Object.entries(options.headers)) {
        headers.push({name, value});
      }
    }
    this.connId = monet.wsConnect(
      url,
      options?.protocols ?? [],
      headers,
      () => {
        this._readyState = 1; // OPEN
        this.onopen?.();
      },
      (event: {isBinary: boolean}) => {
        // Signal: queue the flag, pull the data, fire the event.
        this.pendingFlags.push(event.isBinary);
        this.drainMessages();
      },
      (event: {code: number; reason: string; wasClean: boolean}) => {
        this._readyState = 3; // CLOSED
        this._closed = true;
        const closeEvent: MonetWsCloseEvent = {
          code: event.code,
          reason: event.reason,
          wasClean: event.wasClean,
        };
        if (this._onclose !== null) {
          this._onclose(closeEvent);
        } else {
          this.closeBuffer = closeEvent;
        }
      },
      (event: {kind: string; message: string}) => {
        this._readyState = 3; // CLOSED
        this._closed = true;
        this._onerror?.({kind: event.kind, message: event.message});
      },
    );
  }

  /** 0 CONNECTING, 1 OPEN, 2 CLOSING, 3 CLOSED (WHATWG). */
  get readyState(): number {
    return this._readyState;
  }

  /** The connection URL. */
  get url(): string {
    return this._url;
  }
  private _url: string;

  /** Sends a text or binary message. */
  send(data: string | ArrayBuffer): void {
    if (this._readyState !== 1) {
      throw new Error('MonetWebSocket is not open');
    }
    const monet = getMonet();
    if (typeof data === 'string') {
      // JSI ArrayBuffer from a string: the module copies the bytes.
      const bytes = new TextEncoder().encode(data);
      monet.wsSend(this.connId, bytes.buffer, false);
    } else {
      monet.wsSend(this.connId, data, true);
    }
  }

  /** Sends a ping (the transport answers received pings by itself). */
  ping(): void {
    getMonet().wsSendPing(this.connId);
  }

  /** Starts the close handshake; `onclose` fires when the peer answers. */
  close(code?: number, reason?: string): void {
    if (this._readyState === 1) {
      this._readyState = 2; // CLOSING
      getMonet().wsClose(this.connId, code ?? 1000, reason ?? '');
    }
  }

  /** Force-aborts the connection (no close handshake). */
  abort(): void {
    getMonet().wsAbort(this.connId);
  }

  /** Pulls all queued messages, delivering or buffering each. */
  private drainMessages(): void {
    const monet = getMonet();
    for (;;) {
      const buffer = monet.wsTakeMessage(this.connId);
      if (buffer == null) {
        break;
      }
      const isBinary = this.pendingFlags.shift() ?? false;
      const data: string | ArrayBuffer = isBinary
        ? buffer
        : utf8Decode(buffer);
      const event = {data, isBinary};
      if (this._onmessage !== null) {
        this._onmessage(event);
      } else {
        this.messageBuffer.push(event);
      }
    }
  }
}

/** Decodes UTF-8 bytes into a string (the transport validated them). */
function utf8Decode(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  // TextDecoder is available in RN's Hermes engine.
  return new TextDecoder().decode(bytes);
}
