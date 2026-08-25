import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";
import * as SynchronizedRef from "effect/SynchronizedRef";

export function makeKeyedLock<Key>(options?: { readonly retain?: (key: Key) => boolean }) {
  return Effect.gen(function* () {
    interface Entry {
      readonly semaphore: Semaphore.Semaphore;
      readonly references: number;
    }
    const entries = yield* SynchronizedRef.make(new Map<Key, Entry>());

    const acquire = (key: Key) =>
      SynchronizedRef.modifyEffect(entries, (current) => {
        const existing = current.get(key);
        if (existing !== undefined) {
          const next = new Map(current);
          next.set(key, { ...existing, references: existing.references + 1 });
          return Effect.succeed([existing.semaphore, next] as const);
        }
        return Semaphore.make(1).pipe(
          Effect.map((semaphore) => {
            const next = new Map(current);
            next.set(key, { semaphore, references: 1 });
            return [semaphore, next] as const;
          }),
        );
      });

    const release = (key: Key) =>
      SynchronizedRef.update(entries, (current) => {
        const existing = current.get(key);
        if (existing === undefined) return current;
        const next = new Map(current);
        if (existing.references === 1 && options?.retain?.(key) !== true) {
          next.delete(key);
        } else {
          next.set(key, { ...existing, references: existing.references - 1 });
        }
        return next;
      });

    const withLock = <A, E, R>(key: Key, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      Effect.acquireUseRelease(
        acquire(key),
        (semaphore) => semaphore.withPermit(effect),
        () => release(key),
      );

    const inspect = (key: Key) =>
      SynchronizedRef.get(entries).pipe(
        Effect.map((current) => ({
          keyCount: current.size,
          references: current.get(key)?.references ?? 0,
        })),
      );

    return { withLock, inspect } as const;
  });
}
