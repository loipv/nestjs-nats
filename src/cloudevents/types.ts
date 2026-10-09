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
  extensions?: Record<string, string>;
  mode?: 'binary' | 'structured';
}

export const CE_STRUCTURED_CONTENT_TYPE = 'application/cloudevents+json';
