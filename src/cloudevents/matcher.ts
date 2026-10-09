import { NatsConfigError } from '../errors/errors.js';

interface CompiledRoute<T> {
  segments: string[];
  tail: boolean;
  literals: number;
  exact: boolean;
  index: number;
  target: T;
}

function compile<T>(pattern: string, target: T, index: number): CompiledRoute<T> {
  const parts = pattern.split('.');
  if (parts.some((p) => p === '')) {
    throw new NatsConfigError(`Invalid CloudEvent type pattern "${pattern}": empty segment`);
  }
  const gt = parts.indexOf('>');
  if (gt !== -1 && gt !== parts.length - 1) {
    throw new NatsConfigError(
      `Invalid CloudEvent type pattern "${pattern}": ">" is only allowed as the last segment`,
    );
  }
  const tail = gt !== -1;
  const segments = tail ? parts.slice(0, -1) : parts;
  const literals = segments.filter((s) => s !== '*').length;
  return {
    segments,
    tail,
    literals,
    exact: !tail && literals === segments.length,
    index,
    target,
  };
}

function rank(r: { exact: boolean; tail: boolean }): number {
  if (r.exact) return 0;
  return r.tail ? 2 : 1;
}

function matches<T>(route: CompiledRoute<T>, parts: string[]): boolean {
  if (route.tail ? parts.length <= route.segments.length : parts.length !== route.segments.length) {
    return false;
  }
  return route.segments.every((s, i) => s === '*' || s === parts[i]);
}

export function createTypeRouter<T>(
  routes: Array<{ pattern: string; target: T }>,
): (type: string) => T | undefined {
  const seen = new Set<string>();
  const compiled = routes.map((r, i) => {
    if (seen.has(r.pattern)) {
      throw new NatsConfigError(`Duplicate CloudEvent type pattern "${r.pattern}"`);
    }
    seen.add(r.pattern);
    return compile(r.pattern, r.target, i);
  });
  compiled.sort((a, b) => rank(a) - rank(b) || b.literals - a.literals || a.index - b.index);

  return (type) => {
    const parts = type.split('.');
    if (parts.some((p) => p === '')) return undefined;
    return compiled.find((r) => matches(r, parts))?.target;
  };
}
