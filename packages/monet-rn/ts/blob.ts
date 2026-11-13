// The Blob read surface (v2 patch): RN's Blob has no synchronous read
// path — no arrayBuffer(), only slice()/close() — so the bytes come out
// through FileReader.readAsArrayBuffer. Structural types only: this
// package compiles without the DOM lib, and the runtime check is
// `instanceof Blob` against RN's polyfill (guarded with typeof so a
// runtime without the Blob module degrades to "not a Blob" instead of
// a ReferenceError).
//
// This is deliberately the TS-layer answer to RN Blobs, not a native
// blob-store bridge: the store is RN-internal (BlobModule's map /
// RCTBlobManager) and version-fragile, while the one FileReader copy
// lands exactly where it is acceptable — small blobs. Large payloads
// belong on the {uri} file path, which streams.

/** RN's Blob polyfill shape (Libraries/Blob) — the surface this layer
 *  touches: size, type, and handoff to FileReader. */
export type BlobLike = {
  readonly size: number;
  readonly type: string;
};

// The runtime globals, declared structurally: RN's tsconfig baselines
// carry no DOM lib, so `Blob`/`FileReader` have no global value types
// here. Module-local declarations shadow the DOM globals where they do
// exist — same runtime object either way.
declare const Blob:
  | (new (parts?: ReadonlyArray<string | BlobLike>, options?: {type?: string}) => BlobLike)
  | undefined;

type FileReaderLike = {
  onload: (() => void) | null;
  onerror: (() => void) | null;
  readonly result: ArrayBuffer | string | null;
  readonly error: unknown;
  readAsArrayBuffer(blob: BlobLike): void;
};
declare const FileReader: new () => FileReaderLike;

/** Whether `value` is RN's Blob (the polyfilled global). */
export function isBlob(value: unknown): value is BlobLike {
  return typeof Blob !== 'undefined' && value instanceof Blob;
}

/** Reads a Blob's bytes through FileReader — the only read path RN's
 *  Blob offers. Rejects on reader error; resolves with the full
 *  ArrayBuffer (one copy; see the file header for why that is the
 *  right trade). */
export function readBlob(blob: BlobLike): Promise<ArrayBuffer> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      // readAsArrayBuffer's contract: an ArrayBuffer (RN's FileReader
      // sets it from a typed array's .buffer).
      resolve(reader.result as ArrayBuffer);
    };
    reader.onerror = () => {
      reject(reader.error ?? new Error('FileReader failed without an error'));
    };
    reader.readAsArrayBuffer(blob);
  });
}

/** A Blob as a streaming fetch body: one chunk, read through
 *  FileReader. Keeps `fetch` itself synchronous (it builds options and
 *  hands the stream to the native pump — the async work rides the
 *  pump's iteration). */
export async function* blobBodyStream(
  blob: BlobLike,
): AsyncGenerator<ArrayBuffer> {
  yield await readBlob(blob);
}
