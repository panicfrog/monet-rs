// MonetWebSocket: the EventTarget-style WebSocket over the native
// connection object (callback-signal + zero-copy-pull — the same protocol
// as the fetch layer). The class wiring lives entirely here; the native
// side only fires callbacks.

import { getMonet } from '../generated/rust-turbo/Monet';
import type { MonetWsConnection } from '../generated/rust-turbo/structs/MonetWsConnection';
import { utf8Decode, utf8Encode } from './utf8';

/**
 * Event routing for sockets, in two phases.
 *
 * The native side holds the four event callbacks for the connection's
 * lifetime, and a JSI function handle is a strong GC root in Hermes (the
 * runtime pins every C++-held function in its native-roots list) — so
 * those closures must not capture the socket itself, or the object could
 * never be collected. They are built at MODULE level (see
 * makeDispatchers): an arrow defined in the constructor would close over
 * `this` lexically — the instance — even if its body never mentions it,
 * which is exactly the immortality the NativeState teardown exists to
 * avoid. The closures capture only the correlation id; each event
 * resolves the socket through these maps.
 *
 * CONNECTING sockets are held STRONGLY (`connecting`). The common
 * `await open` pattern keeps the socket alive only through the suspended
 * async frame, whose rooting runs through the very promise the socket's
 * onopen must settle — a cycle with no GC root (the native side's future
 * call is invisible to the collector), so a GC during the handshake
 * would collect the socket mid-connect and the open would never arrive.
 *
 * Once the connection is established the entry turns WEAK
 * (`liveSockets`) — the user's scope holds a socket they are using — so
 * dropping it without close()/abort() stays collectable. The connection
 * object rides the JS wrapper's NativeState: when the collector reclaims
 * the wrapper (the app's own allocation drives it — no explicit
 * collection needed), the connection's Rust `Drop` fires the cancel
 * token and ends the pump. That is the release contract for a dropped
 * socket; the timing is the collector's schedule, so use close() or
 * abort() when immediacy matters. The pump's terminal event (which
 * always fires, even after that cancellation) is what removes a socket's
 * entry here.
 */
const connecting = new Map<number, MonetWebSocket>();
const liveSockets = new Map<number, WeakRef<MonetWebSocket>>();
let nextSocketId = 1;

/** Resolves the socket a native signal belongs to. */
function resolveLive(socketId: number): MonetWebSocket | null {
  const strong = connecting.get(socketId);
  if (strong !== undefined) {
    return strong;
  }
  return liveSockets.get(socketId)?.deref() ?? null;
}

/**
 * The four native event callbacks, built at module level ON PURPOSE: a
 * plain module function's `this` is undefined, so the arrows below
 * capture nothing but the correlation id (arrows in the constructor
 * would capture the instance — see the liveSockets docs).
 */
function makeDispatchers(socketId: number) {
  return {
    onOpen: () => {
      const ws = resolveLive(socketId);
      if (ws !== null) {
        ws.handleOpen();
      }
    },
    onMessage: (event: {isBinary: boolean}) => {
      const ws = resolveLive(socketId);
      if (ws !== null) {
        ws.handleMessageSignal(event);
      }
    },
    onClose: (event: {code: number; reason: string; wasClean: boolean}) => {
      const ws = resolveLive(socketId);
      if (ws !== null) {
        ws.handleClose(event);
      } else {
        // Terminal for a socket that already went away: its Drop
        // disconnected the transport; just retire the entry.
        connecting.delete(socketId);
        liveSockets.delete(socketId);
      }
    },
    onError: (event: {kind: string; message: string}) => {
      const ws = resolveLive(socketId);
      if (ws !== null) {
        ws.handleError(event);
      } else {
        connecting.delete(socketId);
        liveSockets.delete(socketId);
      }
    },
  };
}

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
 * methods) over the native connection object. The `close` event's code
 * and reason round-trip exactly — the fidelity that motivated the iOS
 * transport's hand-rolled framing. Releasing when timing matters is
 * explicit — `close()` for the handshake, `abort()` for an immediate
 * disconnect. A socket dropped without either is not leaked: nothing on
 * the JS side retains it, and the connection's Rust `Drop` (the JS
 * wrapper's NativeState) disconnects once the collector reclaims the
 * wrapper — the app's own allocation drives that, no explicit disposal
 * required.
 */
export class MonetWebSocket {
  /** The native connection object (its lifetime is this object's). */
  private conn: MonetWsConnection;
  /** The correlation id the native-held event closures capture. */
  private socketId: number;
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
  // takeMessage pulls.
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
    // A JS-side correlation id, allocated before the call so the
    // native-held closures capture a plain number and nothing else (see
    // the liveSockets docs — the dispatchers must come from module
    // scope, not from this constructor's lexical `this`). No callback
    // can run before wsConnect returns — they all marshal through the
    // JS thread's queue, and the constructor holds that thread — so
    // publishing the entry afterwards is race-free.
    const socketId = nextSocketId++;
    const dispatchers = makeDispatchers(socketId);
    this.conn = monet.wsConnect(
      url,
      options?.protocols ?? [],
      headers,
      dispatchers.onOpen,
      dispatchers.onMessage,
      dispatchers.onClose,
      dispatchers.onError,
    );
    this.socketId = socketId;
    // Strongly held until the first event (see the connecting docs):
    // the handshake window is exactly when the user's own rooting is
    // the fragile await-open cycle.
    connecting.set(socketId, this);
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
    if (typeof data === 'string') {
      // JSI ArrayBuffer from a string: the module copies the bytes.
      this.conn.send(utf8Encode(data), false);
    } else {
      this.conn.send(data, true);
    }
  }

  /** Sends a ping (the transport answers received pings by itself). */
  ping(): void {
    this.conn.sendPing();
  }

  /** Starts the close handshake; `onclose` fires when the peer answers. */
  close(code?: number, reason?: string): void {
    if (this._readyState === 1) {
      this._readyState = 2; // CLOSING
      // The weak entry stays until the terminal close event: messages
      // may still arrive before the peer's close answer, and a dead
      // entry would drop them.
      this.conn.close(code ?? 1000, reason ?? '');
    }
  }

  /** Force-aborts the connection (no close handshake). */
  abort(): void {
    // The weak entry stays so the terminal event (close 1006 or error)
    // still reaches this object's handlers; it removes itself there.
    this.conn.abort();
  }

  /** Internal: the native on_open dispatch (handshake completed). */
  handleOpen(): void {
    this._readyState = 1; // OPEN
    // Established: the user's scope takes over the rooting (a socket in
    // use is reachable from the code using it), so the entry turns weak
    // and a dropped socket stays collectable.
    connecting.delete(this.socketId);
    liveSockets.set(this.socketId, new WeakRef(this));
    this.onopen?.();
  }

  /**
   * Internal: the native on_message signal — queue the type flag, pull
   * the queued data, deliver or buffer.
   */
  handleMessageSignal(event: {isBinary: boolean}): void {
    this.pendingFlags.push(event.isBinary);
    this.drainMessages();
  }

  /** Internal: terminal close dispatch. */
  handleClose(event: {
    code: number;
    reason: string;
    wasClean: boolean;
  }): void {
    this._readyState = 3; // CLOSED
    this._closed = true;
    connecting.delete(this.socketId);
    liveSockets.delete(this.socketId);
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
  }

  /** Internal: terminal error dispatch. */
  handleError(event: {kind: string; message: string}): void {
    this._readyState = 3; // CLOSED
    this._closed = true;
    connecting.delete(this.socketId);
    liveSockets.delete(this.socketId);
    this._onerror?.({kind: event.kind, message: event.message});
  }

  /** Pulls all queued messages, delivering or buffering each. */
  private drainMessages(): void {
    for (;;) {
      const buffer = this.conn.takeMessage();
      if (buffer == null) {
        break;
      }
      const isBinary = this.pendingFlags.shift() ?? false;
      const data: string | ArrayBuffer = isBinary
        ? buffer
        : utf8Decode(new Uint8Array(buffer));
      const event = {data, isBinary};
      if (this._onmessage !== null) {
        this._onmessage(event);
      } else {
        this.messageBuffer.push(event);
      }
    }
  }
}
