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
 * The same cycle outlives the handshake: `await nextMessage(ws)` (a
 * promise settled from `onmessage`) roots the awaiting frame only
 * through the socket again. So an established socket with a pending
 * consumer — an `onmessage`, `onclose` or `onreconnect` handler set —
 * stays STRONGLY held too (`listenedSockets`; the WHATWG rule that an
 * open socket with event listeners must not be collected). A GC in that
 * window used to clear the weak entry, and the message signal found no
 * socket to deliver to — silently, with the wire healthy.
 *
 * Without such a consumer the entry is WEAK (`liveSockets`) — the
 * user's scope holds a socket they are using — so dropping it without
 * close()/abort() stays collectable. (`onopen`/`onerror` alone do not
 * pin: the open has already fired, and a socket abandoned with an error
 * handler must still be releasable.) The connection
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
const listenedSockets = new Map<number, MonetWebSocket>();
const liveSockets = new Map<number, WeakRef<MonetWebSocket>>();
let nextSocketId = 1;

/** Resolves the socket a native signal belongs to. */
function resolveLive(socketId: number): MonetWebSocket | null {
  const strong = connecting.get(socketId) ?? listenedSockets.get(socketId);
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
  private conn!: MonetWsConnection;
  /** The correlation id the native-held event closures capture. */
  private socketId!: number;
  private _readyState: number = 0; // CONNECTING

  // Event handlers (EventTarget-style property assignment).
  private _onopen: OpenHandler | null = null;
  private _onmessage: MessageHandler | null = null;
  private _onclose: CloseHandler | null = null;
  /** The reconnect policy (v2/W5); null = never reconnect. */
  private _reconnect: {maxAttempts: number; backoffMs: number} | null;
  /** Reconnects spent so far (v2/W5). */
  private _reconnects = 0;
  /** A reconnect scheduled but not yet fired (v2/W5) — close() during
   *  the backoff window cancels it. */
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private _onreconnect: (() => void) | null = null;
  /** The original handshake's parameters (v2/W5) — reconnects dial the
   *  same endpoint with the same protocols and headers. */
  private _reconnectProtocols: string[] = [];
  private _reconnectHeaders: {name: string; value: string}[] = [];
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
    this.updateRetention();
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
    this.updateRetention();
    if (handler !== null && this.closeBuffer !== null) {
      const buffered = this.closeBuffer;
      this.closeBuffer = null;
      handler(buffered);
    }
  }

  /** Fires when a reconnect (v2/W5) re-establishes the connection —
   *  after an abnormal close, before any further message events. */
  get onreconnect(): (() => void) | null {
    return this._onreconnect;
  }
  set onreconnect(handler: (() => void) | null) {
    this._onreconnect = handler;
    this.updateRetention();
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
      /** The reconnect policy (v2/W5): on an ABNORMAL close (1006 or
       *  wasClean false) the socket reconnects with a doubling backoff
       *  up to maxAttempts extra connections. A clean close (the
       *  handshake-completed close() or the server's) never
       *  reconnects. `onreconnect` fires when a reborn connection
       *  opens; the attempts that give out end in the final onclose.
       */
      reconnect?: {maxAttempts: number; backoffMs: number};
    },
  ) {
    this._url = url;
    this._reconnect = options?.reconnect ?? null;
    const headers: {name: string; value: string}[] = [];
    if (options?.headers) {
      for (const [name, value] of Object.entries(options.headers)) {
        headers.push({name, value});
      }
    }
    // The original handshake's parameters, stashed for reconnects
    // (v2/W5): the reborn connection dials the same endpoint. The
    // native wiring lives in spawnConnection (shared with the
    // reconnect path); it allocates the socket id and publishes the
    // strong connect-hold the same way — no callback can run before
    // wsConnect returns (they marshal through the JS thread's queue,
    // and the constructor holds that thread).
    this._reconnectProtocols = options?.protocols ?? [];
    this._reconnectHeaders = headers;
    this.spawnConnection();
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
    // A user close during a reconnect's backoff window cancels it —
    // the policy only covers abnormal closes, never this.
    if (this._reconnectTimer !== null) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
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
    this.updateRetention();
    if (this._reconnects > 0) {
      // A reborn connection (v2/W5): the app learns of the reconnect,
      // not a fresh open.
      this._onreconnect?.();
      return;
    }
    this.onopen?.();
  }

  /** Establishes (or re-establishes, v2/W5) the native connection: the
   *  constructor's wiring, extracted so the reconnect path shares it.
   *  The fresh dispatchers bind a fresh socket id — the old id's
   *  entries were retired at its close. */
  private spawnConnection(): void {
    const monet = getMonet();
    const socketId = nextSocketId++;
    const dispatchers = makeDispatchers(socketId);
    this.conn = monet.wsConnect(
      this._url,
      this._reconnectProtocols,
      this._reconnectHeaders,
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
    connecting.delete(this.socketId);
    listenedSockets.delete(this.socketId);
    liveSockets.delete(this.socketId);
    // The reconnect window (v2/W5): an abnormal close with budget left
    // re-establishes the connection instead of ending the socket. The
    // app sees onreconnect (not onclose) when it lands.
    const abnormal = !event.wasClean || event.code === 1006;
    if (
      abnormal &&
      this._reconnect !== null &&
      this._reconnects < this._reconnect.maxAttempts
    ) {
      this._readyState = 0; // CONNECTING (the rebirth)
      this._reconnects += 1;
      const backoff = this._reconnect.backoffMs << (this._reconnects - 1);
      this._reconnectTimer = setTimeout(() => {
        this._reconnectTimer = null;
        this.spawnConnection();
      }, backoff);
      return;
    }
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
  }

  /** Internal: terminal error dispatch. */
  handleError(event: {kind: string; message: string}): void {
    this._readyState = 3; // CLOSED
    this._closed = true;
    connecting.delete(this.socketId);
    listenedSockets.delete(this.socketId);
    liveSockets.delete(this.socketId);
    this._onerror?.({kind: event.kind, message: event.message});
  }

  /** Pins an established socket strongly while a consumer waits on it
   *  (see the listenedSockets docs); otherwise only the weak entry
   *  remains. CONNECTING is the connecting map's job, CLOSED holds
   *  nothing — and an id whose live entry is gone (a reconnect's
   *  retired id, before the reborn one opens) stays unpinned. */
  private updateRetention(): void {
    const established =
      this._readyState === 1 || this._readyState === 2; // OPEN, CLOSING
    const listening =
      this._onmessage !== null ||
      this._onclose !== null ||
      this._onreconnect !== null;
    if (established && listening && liveSockets.has(this.socketId)) {
      listenedSockets.set(this.socketId, this);
    } else {
      listenedSockets.delete(this.socketId);
    }
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
