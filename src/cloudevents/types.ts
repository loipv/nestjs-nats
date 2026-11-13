export interface CloudEventMeta {
  id: string;
  source: string;
  type: string;
  specversion: string;
  time?: string;
  subject?: string;
  datacontenttype?: string;
  dataschema?: string;
  extensions: Record<string, string>;
}

export interface CloudEventInit {
  type: string;
  source?: string;
  subject?: string;
  id?: string;
  time?: string | Date;
  dataschema?: string;
  /**
   * The media type of the data. Default: the codec's content type, or none when the data
   * is a `Uint8Array` (the library cannot tell what the bytes are).
   */
  datacontenttype?: string;
  extensions?: Record<string, string>;
  /**
   * `'binary'` (default): attributes in headers, data in the body. `'structured'`: one JSON
   * envelope in the body.
   *
   * @experimental `'structured'` may change or be removed before 1.0 (see Stability in the
   * README).
   */
  mode?: 'binary' | 'structured';
}

export const CE_STRUCTURED_CONTENT_TYPE = 'application/cloudevents+json';
