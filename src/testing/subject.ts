/**
 * Whether `subject` matches the NATS subject `pattern`: `*` matches exactly one token,
 * a trailing `>` one or more tokens. A pattern also matches itself verbatim.
 */
export function subjectMatches(pattern: string, subject: string): boolean {
  if (pattern === subject) return true;
  const want = pattern.split('.');
  const got = subject.split('.');
  for (let i = 0; i < want.length; i++) {
    const token = want[i];
    if (token === '>' && i === want.length - 1) return got.length > i;
    if (i >= got.length) return false;
    if (token !== '*' && token !== got[i]) return false;
  }
  return want.length === got.length;
}
