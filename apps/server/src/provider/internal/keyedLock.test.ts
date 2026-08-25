import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";

import { makeKeyedLock } from "./keyedLock.ts";

it.effect("serializes equal keys without blocking independent keys and releases idle entries", () =>
  Effect.gen(function* () {
    const lock = yield* makeKeyedLock<string>();
    const firstEntered = yield* Deferred.make<void>();
    const releaseFirst = yield* Deferred.make<void>();
    const order = yield* Ref.make<Array<string>>([]);

    const first = yield* lock
      .withLock(
        "shared",
        Ref.update(order, (current) => [...current, "first-entered"]).pipe(
          Effect.andThen(Deferred.succeed(firstEntered, undefined)),
          Effect.andThen(Deferred.await(releaseFirst)),
          Effect.andThen(Ref.update(order, (current) => [...current, "first-exited"])),
        ),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(firstEntered);

    const second = yield* lock
      .withLock(
        "shared",
        Ref.update(order, (current) => [...current, "second-entered"]),
      )
      .pipe(Effect.forkScoped);

    yield* lock.withLock(
      "independent",
      Ref.update(order, (current) => [...current, "independent-entered"]),
    );
    assert.deepEqual(yield* Ref.get(order), ["first-entered", "independent-entered"]);

    yield* Deferred.succeed(releaseFirst, undefined);
    yield* Fiber.join(first);
    yield* Fiber.join(second);
    assert.deepEqual(yield* Ref.get(order), [
      "first-entered",
      "independent-entered",
      "first-exited",
      "second-entered",
    ]);

    assert.deepEqual(yield* lock.inspect("shared"), { keyCount: 0, references: 0 });
    assert.deepEqual(yield* lock.inspect("independent"), { keyCount: 0, references: 0 });
  }).pipe(Effect.scoped),
);
