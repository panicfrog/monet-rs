import {utf8Encode} from './utf8';

// MonetFormData (v2/W5): the multipart/form-data assembly for the TS
// fetch layer. string and ArrayBuffer parts serialize inline; a file
// part ({uri, name, type} — RN's asset shape) needs a native read
// surface that v2 does not carry yet and throws with that message (the
// recorded growth path is the Blob bridge).

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
  | {kind: 'bytes'; name: string; value: ArrayBuffer; filename?: string};

export class MonetFormData {
  private parts: Part[] = [];
  private boundary = randomBoundary();

  /** Appends one field. A string is text; an ArrayBuffer is a file-ish
   *  part (filename optional). RN's {uri} file shape is not supported
   *  yet — it throws (see the file header). */
  append(
    name: string,
    value: string | ArrayBuffer | {uri: string; name?: string; type?: string},
  ): void {
    if (typeof value === 'string') {
      this.parts.push({kind: 'field', name, value});
    } else if (value instanceof ArrayBuffer) {
      this.parts.push({kind: 'bytes', name, value, filename: undefined});
    } else {
      throw new Error(
        'MonetFormData: file parts ({uri}) need the Blob bridge (not in v2) — ' +
          'read the asset and append an ArrayBuffer',
      );
    }
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
      } else {
        const filename = part.filename ?? part.name;
        pushText(
          `Content-Disposition: form-data; name="${part.name}"; ` +
            `filename="${filename}"\r\n` +
            `Content-Type: application/octet-stream\r\n\r\n`,
        );
        chunks.push(new Uint8Array(part.value));
        pushText('\r\n');
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
      contentType: `multipart/form-data; boundary=${this.boundary}`,
    };
  }
}
