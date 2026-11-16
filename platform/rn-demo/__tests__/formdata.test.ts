/**
 * MonetFormData's multipart assembly (serialize() is pure JS).
 */

import {MonetFormData} from '../../../packages/monet-rn/ts/formdata';
import {utf8Decode} from '../../../packages/monet-rn/ts/utf8';

function serialized(form: MonetFormData): string {
  return utf8Decode(new Uint8Array(form.serialize().body));
}

function serializedBytes(form: MonetFormData): number[] {
  return Array.from(new Uint8Array(form.serialize().body));
}

/** The bytes between a part's blank line and its closing CRLF. */
function partPayload(form: MonetFormData): number[] {
  const bytes = serializedBytes(form);
  const text = String.fromCharCode(...bytes);
  const start = text.indexOf('\r\n\r\n') + 4;
  const end = text.indexOf('\r\n--', start);
  return bytes.slice(start, end);
}

describe('names, filenames and types are escaped (the HTML multipart rules)', () => {
  test('a field name cannot break out of its quotes or inject a header', () => {
    const form = new MonetFormData();
    form.append('a"b\r\nX-Injected: 1', 'v');
    const text = serialized(form);
    expect(text).toContain('Content-Disposition: form-data; name="a%22b%0D%0AX-Injected: 1"\r\n');
    expect(text).not.toMatch(/^X-Injected/m);
  });

  test('a filename and a content type are escaped too', () => {
    const form = new MonetFormData();
    form.append('file', new ArrayBuffer(1), {
      filename: 'x".bin\r\nX-Evil: 1',
      contentType: 'text/plain\r\nX-Evil: 2',
    });
    const text = serialized(form);
    expect(text).toContain('filename="x%22.bin%0D%0AX-Evil: 1"\r\n');
    expect(text).toContain('Content-Type: text/plain%0D%0AX-Evil: 2\r\n');
    expect(text).not.toMatch(/^X-Evil/m);
  });

  test('ordinary names are untouched', () => {
    const form = new MonetFormData();
    form.append('plain_name-1', 'v');
    expect(serialized(form)).toContain('name="plain_name-1"\r\n');
  });
});

describe('typed-array views are binary parts', () => {
  test('a Uint8Array serializes inline as its bytes', () => {
    const form = new MonetFormData();
    form.append('bytes', new Uint8Array([1, 2, 3]));
    expect(form.hasFileParts()).toBe(false);
    expect(partPayload(form)).toEqual([1, 2, 3]);
  });

  test('a view sends exactly its window of the buffer', () => {
    const form = new MonetFormData();
    form.append('bytes', new Uint8Array([9, 1, 2, 3, 9]).subarray(1, 4));
    expect(partPayload(form)).toEqual([1, 2, 3]);
  });

  test('a value of no supported shape is refused at append', () => {
    const form = new MonetFormData();
    expect(() => form.append('x', {name: 'no-uri'} as any)).toThrow(TypeError);
  });
});
