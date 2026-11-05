// Minimal UTF-8 codecs: RN's JS runtime does not guarantee the WHATWG
// TextEncoder/TextDecoder globals, and the fetch layer must not depend on
// polyfills the host app may or may not install. Correctness over speed —
// battery bodies are small; malformed input degrades to U+FFFD.

declare global {
  // Present in some JS runtimes; declared structurally so the fast path
  // type-checks without the DOM lib.
  var TextEncoder: {new (): {encode(text: string): Uint8Array}} | undefined;
  var TextDecoder: {new (label: string): {decode(bytes: Uint8Array): string}} | undefined;
}

/** UTF-8 encodes into a fresh ArrayBuffer (the upload body format). */
export function utf8Encode(text: string): ArrayBuffer {
  if (typeof TextEncoder !== 'undefined') {
    return new TextEncoder().encode(text).buffer as ArrayBuffer;
  }
  const out: number[] = [];
  for (let i = 0; i < text.length; i++) {
    let code = text.charCodeAt(i);
    if (code >= 0xd800 && code <= 0xdbff && i + 1 < text.length) {
      const next = text.charCodeAt(i + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        code = 0x10000 + ((code - 0xd800) << 10) + (next - 0xdc00);
        i++;
      }
    }
    if (code < 0x80) {
      out.push(code);
    } else if (code < 0x800) {
      out.push(0xc0 | (code >> 6), 0x80 | (code & 0x3f));
    } else if (code < 0x10000) {
      out.push(
        0xe0 | (code >> 12),
        0x80 | ((code >> 6) & 0x3f),
        0x80 | (code & 0x3f),
      );
    } else {
      out.push(
        0xf0 | (code >> 18),
        0x80 | ((code >> 12) & 0x3f),
        0x80 | ((code >> 6) & 0x3f),
        0x80 | (code & 0x3f),
      );
    }
  }
  const bytes = new Uint8Array(out.length);
  for (let i = 0; i < out.length; i++) {
    bytes[i] = out[i];
  }
  return bytes.buffer;
}

/** UTF-8 decodes; malformed or truncated sequences become U+FFFD. */
export function utf8Decode(bytes: Uint8Array): string {
  if (typeof TextDecoder !== 'undefined') {
    return new TextDecoder('utf-8').decode(bytes);
  }
  let out = '';
  let i = 0;
  const length = bytes.length;
  while (i < length) {
    const b = bytes[i];
    if (b < 0x80) {
      out += String.fromCharCode(b);
      i++;
      continue;
    }
    let codePoint: number;
    let extra: number;
    if (b >= 0xc2 && b <= 0xdf) {
      codePoint = b & 0x1f;
      extra = 1;
    } else if (b >= 0xe0 && b <= 0xef) {
      codePoint = b & 0x0f;
      extra = 2;
    } else if (b >= 0xf0 && b <= 0xf4) {
      codePoint = b & 0x07;
      extra = 3;
    } else {
      out += '�';
      i++;
      continue;
    }
    if (i + extra >= length) {
      out += '�';
      break;
    }
    let valid = true;
    for (let k = 1; k <= extra; k++) {
      const cont = bytes[i + k];
      if ((cont & 0xc0) !== 0x80) {
        valid = false;
        break;
      }
      codePoint = (codePoint << 6) | (cont & 0x3f);
    }
    if (!valid) {
      out += '�';
      i++;
      continue;
    }
    if (codePoint <= 0xffff) {
      out += String.fromCharCode(codePoint);
    } else {
      codePoint -= 0x10000;
      out += String.fromCharCode(
        0xd800 + (codePoint >> 10),
        0xdc00 + (codePoint & 0x3ff),
      );
    }
    i += extra + 1;
  }
  return out;
}
