import {utf8Encode} from './utf8';

// MonetFormData (v2/W5): the multipart/form-data assembly for the TS
// fetch layer. string and ArrayBuffer parts serialize inline; a file
// part ({uri, name, type}) streams: the framing yields as strings and
// the file's bytes as ArrayBuffer chunks read natively
// (Monet.readFileChunk — std::fs under a file:// URI or bare path), so
// a large upload never crosses into JS memory whole. Non-file schemes
// (content://, ph://) fail in the native reader — the recorded growth
// paths.

import { getMonet } from '../generated/rust-turbo/Monet';

const BOUNDARY_CHARS =
  'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

/** A random boundary, 16 chars — collision-safe for one body. */
function randomBoundary(): string {
  let out = '';
  for (let i = 0; i < 16; i++) {
    out += BOUNDARY_CHARS[Math.floor(Math.random() * BOUNDARY_CHARS.length)];
  }
  return `----monet${out}`;
}

type Part =
  | {kind: 'field'; name: string; value: string}
  | {
      kind: 'bytes';
      name: string;
      value: ArrayBuffer;
      filename?: string;
      contentType?: string;
    }
  | {
      kind: 'file';
      name: string;
      uri: string;
      filename: string;
      contentType: string;
    };

export class MonetFormData {
  private parts: Part[] = [];
  private boundary = randomBoundary();

  /** Appends one field: a string (text), an ArrayBuffer (a binary part;
   *  `meta` carries its filename/content-type the way browsers'
   *  append(name, blob, filename) does), or a file part — `{uri, name,
   *  type}` where uri is a file:// URL or bare path the native reader
   *  streams (see the file header). */
  append(
    name: string,
    value: string | ArrayBuffer | {uri: string; name?: string; type?: string},
    meta?: {filename?: string; contentType?: string},
  ): void {
    if (typeof value === 'string') {
      this.parts.push({kind: 'field', name, value});
    } else if (value instanceof ArrayBuffer) {
      this.parts.push({
        kind: 'bytes',
        name,
        value,
        filename: meta?.filename,
        contentType: meta?.contentType,
      });
    } else {
      this.parts.push({
        kind: 'file',
        name,
        uri: value.uri,
        filename: value.name ?? name,
        contentType: value.type ?? 'application/octet-stream',
      });
    }
  }

  /** The content-type header value (the boundary is per-form). */
  contentType(): string {
    return `multipart/form-data; boundary=${this.boundary}`;
  }

  /** Whether any part streams from the filesystem (the fetch layer
   *  routes such forms through the streaming upload). */
  hasFileParts(): boolean {
    return this.parts.some(part => part.kind === 'file');
  }

  /** The streaming form (v2/W5 patch): the same bytes `serialize()`
   *  would produce, yielded as strings (framing) and ArrayBuffers
   *  (file chunks read natively, 256KB at a time). */
  async *stream(): AsyncGenerator<string | ArrayBuffer> {
    const monet = getMonet();
    for (const part of this.parts) {
      yield `--${this.boundary}\r\n`;
      if (part.kind === 'field') {
        yield `Content-Disposition: form-data; name="${part.name}"\r\n\r\n`;
        yield `${part.value}\r\n`;
      } else if (part.kind === 'bytes') {
        yield this.bytesPartHeaders(part.name, part.filename, part.contentType);
        yield part.value;
        yield '\r\n';
      } else {
        yield this.bytesPartHeaders(part.name, part.filename, part.contentType);
        let offset = 0;
        for (;;) {
          const chunk = monet.readFileChunk(part.uri, offset, 262144);
          if (chunk == null) {
            break; // EOF
          }
          yield chunk;
          offset += chunk.byteLength;
        }
        yield '\r\n';
      }
    }
    yield `--${this.boundary}--\r\n`;
  }

  private bytesPartHeaders(
    name: string,
    filename: string | undefined,
    contentType: string | undefined,
  ): string {
    const name_ = filename ?? name;
    return (
      `Content-Disposition: form-data; name="${name}"; ` +
      `filename="${name_}"\r\n` +
      `Content-Type: ${contentType ?? 'application/octet-stream'}\r\n\r\n`
    );
  }

  /** The assembled body and its content-type header value. */
  serialize(): {body: ArrayBuffer; contentType: string} {
    const chunks: Uint8Array[] = [];
    const pushText = (text: string) =>
      chunks.push(new Uint8Array(utf8Encode(text)));

    for (const part of this.parts) {
      pushText(`--${this.boundary}\r\n`);
      if (part.kind === 'field') {
        pushText(
          `Content-Disposition: form-data; name="${part.name}"\r\n` +
            `\r\n${part.value}\r\n`,
        );
      } else if (part.kind === 'bytes') {
        pushText(
          this.bytesPartHeaders(part.name, part.filename, part.contentType),
        );
        chunks.push(new Uint8Array(part.value));
        pushText('\r\n');
      } else {
        // Unreachable from the fetch layer (file parts route through
        // stream()); a direct serialize() call fails loudly instead of
        // silently dropping the part.
        throw new Error(
          'MonetFormData: a file part cannot serialize inline — send it through fetch (it streams)',
        );
      }
    }
    pushText(`--${this.boundary}--\r\n`);

    const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
    const body = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return {
      body: body.buffer,
      contentType: this.contentType(),
    };
  }
}
