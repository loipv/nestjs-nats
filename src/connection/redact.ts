const DEFAULT_SERVER = 'nats://127.0.0.1:4222';

export function redactServers(servers: string | string[] | undefined): string[] {
  if (servers === undefined) return [DEFAULT_SERVER];
  const list = Array.isArray(servers) ? servers : [servers];
  return list.map(redactServer);
}

function redactServer(server: string): string {
  // Userinfo is everything between the scheme (if any) and the last '@' before the host.
  // Passwords may legally contain '@', so match up to the last '@' in the authority part.
  return server.replace(
    /^([a-z][a-z0-9+.-]*:\/\/)?[^/?#]*@/i,
    (_m, scheme?: string) => `${scheme ?? ''}***@`,
  );
}
