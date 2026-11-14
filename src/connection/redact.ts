const DEFAULT_SERVER = 'nats://127.0.0.1:4222';

export function redactServers(servers: string | string[] | undefined): string[] {
  if (servers === undefined) return [DEFAULT_SERVER];
  const list = Array.isArray(servers) ? servers : [servers];
  return list.map(redactServer);
}

// Userinfo is everything between the scheme (if any) and the last '@' before the host.
// Passwords may legally contain '@', so match up to the last '@' in the authority part.
const USERINFO = /^([a-z][a-z0-9+.-]*:\/\/)?([^/?#]*)@/i;

function redactServer(server: string): string {
  return server.replace(USERINFO, (_m, scheme?: string) => `${scheme ?? ''}***@`);
}

/**
 * The secrets embedded in server URLs (the password of `user:pass@`, or a bare token), for
 * masking them in free text that `redactText`'s URL pattern misses (e.g. a malformed URL).
 */
export function serverSecrets(servers: string | string[] | undefined): string[] {
  if (servers === undefined) return [];
  const list = Array.isArray(servers) ? servers : [servers];
  return list.flatMap((server) => {
    const userinfo = USERINFO.exec(server)?.[2];
    if (!userinfo) return [];
    const colon = userinfo.indexOf(':');
    const secret = colon === -1 ? userinfo : userinfo.slice(colon + 1);
    return secret ? [secret] : [];
  });
}

/**
 * Safety net for free text (e.g. error messages): masks URL userinfo and any known secrets.
 */
export function redactText(text: string, secrets: Array<string | undefined> = []): string {
  let out = text.replace(/([a-z][a-z0-9+.-]*:\/\/)[^\s/?#]*@/gi, '$1***@');
  // Longest first: masking a secret contained in a longer one would leave the rest visible.
  const known = secrets.filter((s): s is string => !!s).sort((a, b) => b.length - a.length);
  for (const secret of known) out = out.split(secret).join('***');
  return out;
}

/** The secrets of a connection's options: its `pass`, its `token`, and those of its server URLs. */
export function connectionSecrets(connection: {
  pass?: string;
  token?: string;
  servers?: string | string[];
}): Array<string | undefined> {
  return [connection.pass, connection.token, ...serverSecrets(connection.servers)];
}

const MAX_CAUSE_DEPTH = 5;

/**
 * A sanitized copy of `err`, safe to expose as a `cause`: keeps `name`, `code` and the
 * message and stack with credentials masked (as `redactText`), and sanitizes the `cause`
 * chain the same way. Every other own property is dropped: the raw error may carry
 * credentials in it, such as the `input` URL of Node's `ERR_INVALID_URL`.
 */
export function redactError(err: unknown, secrets: Array<string | undefined> = []): Error {
  return redactErrorAt(err, secrets, 0);
}

function redactErrorAt(err: unknown, secrets: Array<string | undefined>, depth: number): Error {
  if (!(err instanceof Error)) return new Error(redactText(String(err), secrets));
  const safe = new Error(redactText(err.message, secrets));
  safe.name = err.name;
  const { code } = err as { code?: unknown };
  if (typeof code === 'string' || typeof code === 'number') Object.assign(safe, { code });
  safe.stack = err.stack === undefined ? undefined : redactText(err.stack, secrets);
  if (err.cause !== undefined && depth < MAX_CAUSE_DEPTH) {
    safe.cause = redactErrorAt(err.cause, secrets, depth + 1);
  }
  return safe;
}
