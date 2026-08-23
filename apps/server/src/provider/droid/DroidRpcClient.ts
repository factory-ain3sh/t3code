import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  DroidAskUserRequest,
  DroidPermissionRequest,
  DroidSessionNotification,
  type DroidAskUserRequest as DroidAskUserRequestType,
  type DroidPermissionRequest as DroidPermissionRequestType,
  type DroidSessionNotification as DroidSessionNotificationType,
} from "./DroidProtocol.ts";

const defaultRequestTimeoutMs = 30_000;
const gracefulShutdownTimeout = Duration.seconds(2);

export interface DroidRpcSpawnInput {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd?: string;
  readonly env?: NodeJS.ProcessEnv;
}

export interface DroidProcessExit {
  readonly code: number | null;
  readonly signal?: string;
  readonly description: string;
}

export class DroidRpcSpawnError extends Data.TaggedError("DroidRpcSpawnError")<{
  readonly command: string;
  readonly cause: unknown;
}> {}

export class DroidRpcError extends Data.TaggedError("DroidRpcError")<{
  readonly kind: "encode" | "write" | "timeout" | "rpc" | "process-exit" | "duplicate-response";
  readonly message: string;
  readonly method?: string;
  readonly requestId?: string;
  readonly code?: number;
  readonly data?: unknown;
  readonly cause?: unknown;
}> {}

export interface DroidRpcDiagnostic {
  readonly level: "warning";
  readonly message: string;
  readonly line?: string;
  readonly cause?: unknown;
}

interface DroidServerRequestBase {
  readonly id: string;
  readonly sessionId: string | undefined;
  readonly respond: (result: unknown) => Effect.Effect<void, DroidRpcError>;
  readonly fail: (code: number, message: string) => Effect.Effect<void, DroidRpcError>;
}

export interface DroidPermissionServerRequest extends DroidServerRequestBase {
  readonly method: "droid.request_permission";
  readonly params: DroidPermissionRequestType;
}

export interface DroidAskUserServerRequest extends DroidServerRequestBase {
  readonly method: "droid.ask_user";
  readonly params: DroidAskUserRequestType;
}

export type DroidServerRequest = DroidPermissionServerRequest | DroidAskUserServerRequest;

export interface DroidNotificationEnvelope {
  readonly sessionId: string | undefined;
  readonly notification: DroidSessionNotificationType;
}

export interface DroidRpcClient {
  readonly request: (
    method: string,
    params: unknown,
    options?: { readonly timeoutMs?: number | undefined },
  ) => Effect.Effect<unknown, DroidRpcError>;
  readonly notifications: Stream.Stream<DroidNotificationEnvelope>;
  readonly serverRequests: Stream.Stream<DroidServerRequest>;
  readonly diagnostics: Stream.Stream<DroidRpcDiagnostic>;
  readonly exits: Effect.Effect<DroidProcessExit>;
  readonly shutdown: Effect.Effect<void>;
}

export interface ParsedJsonRpcMessage {
  readonly jsonrpc: "2.0";
  readonly type?: "request" | "response" | "notification";
  readonly id?: string | number | null;
  readonly method?: string;
  readonly params?: unknown;
  readonly result?: unknown;
  readonly error?: unknown;
  readonly [key: string]: unknown;
}

export type ParseJsonRpcLineResult =
  | { readonly _tag: "Message"; readonly message: ParsedJsonRpcMessage }
  | { readonly _tag: "Invalid"; readonly error: string };

export function parseJsonRpcLine(line: string): ParseJsonRpcLineResult {
  try {
    const parsed: unknown = JSON.parse(line);
    if (!Predicate.isObject(parsed) || Array.isArray(parsed)) {
      return { _tag: "Invalid", error: "JSON-RPC line must contain an object" };
    }
    if (parsed.jsonrpc !== "2.0") {
      return { _tag: "Invalid", error: 'JSON-RPC line must include jsonrpc: "2.0"' };
    }
    if (
      parsed.type !== undefined &&
      parsed.type !== "request" &&
      parsed.type !== "response" &&
      parsed.type !== "notification"
    ) {
      return { _tag: "Invalid", error: "JSON-RPC line has an invalid type discriminator" };
    }
    return {
      _tag: "Message",
      message: {
        ...parsed,
        jsonrpc: "2.0",
      },
    };
  } catch (cause) {
    return {
      _tag: "Invalid",
      error: cause instanceof Error ? cause.message : String(cause),
    };
  }
}

export interface NdjsonChunkSplitter {
  readonly push: (chunk: string) => ReadonlyArray<string>;
  readonly end: () => ReadonlyArray<string>;
}

export function splitNdjsonChunks(): NdjsonChunkSplitter {
  let pending = "";

  const normalize = (line: string) => (line.endsWith("\r") ? line.slice(0, -1) : line);

  return {
    push: (chunk) => {
      pending += chunk;
      const parts = pending.split("\n");
      pending = parts.pop() ?? "";
      return parts.map(normalize).filter((line) => line.trim().length > 0);
    },
    end: () => {
      const tail = normalize(pending);
      pending = "";
      return tail.trim().length > 0 ? [tail] : [];
    },
  };
}

interface PendingRequest {
  readonly _tag: "Pending";
  readonly method: string;
  readonly deferred: Deferred.Deferred<unknown, DroidRpcError>;
}

interface TimedOutRequest {
  readonly _tag: "TimedOut";
  readonly method: string;
}

type RequestState = PendingRequest | TimedOutRequest;

type DroidRpcLifecycle =
  | {
      readonly _tag: "Running";
      readonly pending: ReadonlyMap<string, RequestState>;
    }
  | {
      readonly _tag: "ShuttingDown";
      readonly pending: ReadonlyMap<string, RequestState>;
      readonly exit?: DroidProcessExit;
    }
  | {
      readonly _tag: "Exited";
      readonly exit: DroidProcessExit;
    };

const decodeNotification = Schema.decodeUnknownEffect(DroidSessionNotification);
const decodePermissionRequest = Schema.decodeUnknownEffect(DroidPermissionRequest);
const decodeAskUserRequest = Schema.decodeUnknownEffect(DroidAskUserRequest);
const encodeJsonRpcMessage = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

function jsonRpcErrorFromMessage(error: unknown, method: string, requestId: string): DroidRpcError {
  if (Predicate.isObject(error) && typeof error.message === "string") {
    return new DroidRpcError({
      kind: "rpc",
      message: error.message,
      method,
      requestId,
      ...(typeof error.code === "number" ? { code: error.code } : {}),
      ...("data" in error ? { data: error.data } : {}),
    });
  }
  return new DroidRpcError({
    kind: "rpc",
    message: "Droid returned an invalid JSON-RPC error response",
    method,
    requestId,
    data: error,
  });
}

export const makeDroidRpcClient = (
  input: DroidRpcSpawnInput,
): Effect.Effect<
  DroidRpcClient,
  DroidRpcSpawnError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const runtimeScope = yield* Scope.Scope;
    const outgoing = yield* Queue.unbounded<string, Cause.Done<void>>();
    const notificationPubSub = yield* Queue.unbounded<
      DroidNotificationEnvelope,
      Cause.Done<void>
    >();
    const serverRequestPubSub = yield* Queue.unbounded<DroidServerRequest, Cause.Done<void>>();
    const diagnosticPubSub = yield* Queue.sliding<DroidRpcDiagnostic, Cause.Done<void>>(256);
    const lifecycle = yield* SynchronizedRef.make<DroidRpcLifecycle>({
      _tag: "Running",
      pending: new Map(),
    });
    const nextRequestId = yield* Ref.make(0);
    const exitDeferred = yield* Deferred.make<DroidProcessExit>();

    const publishDiagnostic = (diagnostic: DroidRpcDiagnostic) =>
      Effect.logWarning(diagnostic.message, {
        ...(diagnostic.line === undefined ? {} : { line: diagnostic.line }),
        ...(diagnostic.cause === undefined ? {} : { cause: diagnostic.cause }),
      }).pipe(Effect.andThen(Queue.offer(diagnosticPubSub, diagnostic)), Effect.asVoid);

    const spawnCommand = yield* resolveSpawnCommand(
      input.command,
      input.args,
      input.env ? { env: input.env, extendEnv: true } : {},
    );
    const child = yield* spawner
      .spawn(
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          ...(input.cwd ? { cwd: input.cwd } : {}),
          ...(input.env ? { env: input.env, extendEnv: true } : {}),
          shell: spawnCommand.shell,
          stdin: {
            stream: Stream.encodeText(Stream.fromQueue(outgoing)),
            endOnDone: true,
          },
        }),
      )
      .pipe(
        Effect.provideService(Scope.Scope, runtimeScope),
        Effect.mapError(
          (cause) =>
            new DroidRpcSpawnError({
              command: input.command,
              cause,
            }),
        ),
      );

    const writeEnvelope = (message: Record<string, unknown>): Effect.Effect<void, DroidRpcError> =>
      encodeJsonRpcMessage(message).pipe(
        Effect.map((encoded) => `${encoded}\n`),
        Effect.mapError(
          (cause) =>
            new DroidRpcError({
              kind: "encode",
              message: "Failed to encode Droid JSON-RPC message",
              cause,
            }),
        ),
        Effect.flatMap((encoded) => Queue.offer(outgoing, encoded)),
        Effect.flatMap((offered) =>
          offered
            ? Effect.void
            : Effect.fail(
                new DroidRpcError({
                  kind: "write",
                  message: "Failed to write to Droid process stdin because it is closed",
                }),
              ),
        ),
      );

    const sendResponse = (
      id: string,
      result:
        | { readonly _tag: "Success"; readonly value: unknown }
        | { readonly _tag: "Failure"; readonly code: number; readonly message: string },
    ) =>
      writeEnvelope({
        jsonrpc: "2.0",
        type: "response",
        factoryApiVersion: "1.0.0",
        id,
        ...(result._tag === "Success"
          ? { result: result.value }
          : { error: { code: result.code, message: result.message } }),
      });

    const resolveResponse = (message: ParsedJsonRpcMessage): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (message.id === undefined || message.id === null) {
          yield* publishDiagnostic({
            level: "warning",
            message: "Ignoring Droid JSON-RPC response without an id",
          });
          return;
        }
        const requestId = String(message.id);
        const requestState = yield* SynchronizedRef.modify(lifecycle, (state) => {
          if (state._tag === "Exited") {
            return [undefined, state] as const;
          }
          const found = state.pending.get(requestId);
          if (!found) {
            return [undefined, state] as const;
          }
          const next = new Map(state.pending);
          next.delete(requestId);
          return [found, { ...state, pending: next }] as const;
        });
        if (!requestState) {
          yield* publishDiagnostic({
            level: "warning",
            message: `Ignoring response for unknown Droid request ${requestId}`,
          });
          return;
        }
        if (requestState._tag === "TimedOut") {
          const error = new DroidRpcError({
            kind: "duplicate-response",
            message: `Droid request ${requestState.method} responded after timing out`,
            method: requestState.method,
            requestId,
          });
          yield* publishDiagnostic({
            level: "warning",
            message: error.message,
            cause: error,
          });
          return;
        }
        if (message.error !== undefined) {
          yield* Deferred.fail(
            requestState.deferred,
            jsonRpcErrorFromMessage(message.error, requestState.method, requestId),
          );
          return;
        }
        yield* Deferred.succeed(requestState.deferred, message.result);
      });

    const publishServerRequest = (
      id: string,
      sessionId: string | undefined,
      method: "droid.request_permission" | "droid.ask_user",
      params: DroidPermissionRequestType | DroidAskUserRequestType,
    ) => {
      const base = {
        id,
        sessionId,
        respond: (result: unknown) =>
          sendResponse(id, {
            _tag: "Success",
            value: result,
          }),
        fail: (code: number, message: string) =>
          sendResponse(id, {
            _tag: "Failure",
            code,
            message,
          }),
      };
      const request =
        method === "droid.request_permission"
          ? ({
              ...base,
              method,
              params: params as DroidPermissionRequestType,
            } satisfies DroidPermissionServerRequest)
          : ({
              ...base,
              method,
              params: params as DroidAskUserRequestType,
            } satisfies DroidAskUserServerRequest);
      return Queue.offer(serverRequestPubSub, request).pipe(Effect.asVoid);
    };

    const handleServerRequest = (message: ParsedJsonRpcMessage): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (message.id === undefined || message.id === null || typeof message.method !== "string") {
          yield* publishDiagnostic({
            level: "warning",
            message: "Ignoring malformed server-initiated Droid request",
          });
          return;
        }
        const id = String(message.id);
        const sessionId =
          Predicate.isObject(message.params) && typeof message.params.sessionId === "string"
            ? message.params.sessionId
            : undefined;
        if (message.method === "droid.request_permission") {
          const decoded = yield* decodePermissionRequest(message.params).pipe(Effect.result);
          if (decoded._tag === "Failure") {
            yield* publishDiagnostic({
              level: "warning",
              message: "Unable to decode droid.request_permission params",
              cause: decoded.failure,
            });
            yield* sendResponse(id, {
              _tag: "Failure",
              code: -32602,
              message: "Invalid droid.request_permission params",
            }).pipe(Effect.ignore);
            return;
          }
          yield* publishServerRequest(id, sessionId, message.method, decoded.success);
          return;
        }
        if (message.method === "droid.ask_user") {
          const decoded = yield* decodeAskUserRequest(message.params).pipe(Effect.result);
          if (decoded._tag === "Failure") {
            yield* publishDiagnostic({
              level: "warning",
              message: "Unable to decode droid.ask_user params",
              cause: decoded.failure,
            });
            yield* sendResponse(id, {
              _tag: "Failure",
              code: -32602,
              message: "Invalid droid.ask_user params",
            }).pipe(Effect.ignore);
            return;
          }
          yield* publishServerRequest(id, sessionId, message.method, decoded.success);
          return;
        }
        yield* publishDiagnostic({
          level: "warning",
          message: `Ignoring unsupported server-initiated Droid request ${message.method}`,
        });
        yield* sendResponse(id, {
          _tag: "Failure",
          code: -32601,
          message: `Unsupported Droid request: ${message.method}`,
        }).pipe(Effect.ignore);
      });

    const handleNotification = (message: ParsedJsonRpcMessage): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (message.method !== "droid.session_notification") {
          return;
        }
        if (!Predicate.isObject(message.params)) {
          yield* publishDiagnostic({
            level: "warning",
            message: "Ignoring Droid session notification with invalid params",
          });
          return;
        }
        const decoded = yield* decodeNotification(message.params.notification).pipe(Effect.result);
        if (decoded._tag === "Failure") {
          yield* publishDiagnostic({
            level: "warning",
            message: "Unable to decode Droid session notification",
            cause: decoded.failure,
          });
          return;
        }
        const sessionId =
          typeof message.params.sessionId === "string" ? message.params.sessionId : undefined;
        yield* Queue.offer(notificationPubSub, {
          sessionId,
          notification: decoded.success,
        });
      });

    const handleMessage = (message: ParsedJsonRpcMessage): Effect.Effect<void> => {
      const isRequest =
        message.type === "request" ||
        (message.type === undefined &&
          typeof message.method === "string" &&
          message.id !== undefined);
      const isNotification =
        message.type === "notification" ||
        (message.type === undefined &&
          typeof message.method === "string" &&
          message.id === undefined);
      if (isRequest) {
        return handleServerRequest(message);
      }
      if (isNotification) {
        return handleNotification(message);
      }
      return resolveResponse(message);
    };

    const splitter = splitNdjsonChunks();
    const handleLine = (line: string) => {
      const parsed = parseJsonRpcLine(line);
      return parsed._tag === "Invalid"
        ? publishDiagnostic({
            level: "warning",
            message: `Unable to parse Droid JSON-RPC line: ${parsed.error}`,
            line,
          })
        : handleMessage(parsed.message);
    };

    const stdoutFiber = yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Effect.forEach(splitter.push(chunk), handleLine, {
          discard: true,
        }),
      ),
      Effect.andThen(
        Effect.forEach(splitter.end(), handleLine, {
          discard: true,
        }),
      ),
      Effect.catch((cause) =>
        publishDiagnostic({
          level: "warning",
          message: "Droid stdout stream failed",
          cause,
        }),
      ),
      Effect.forkIn(runtimeScope),
    );

    const stderrFiber = yield* child.stderr.pipe(
      Stream.decodeText(),
      Stream.runForEach((output) =>
        output.trim().length === 0
          ? Effect.void
          : publishDiagnostic({
              level: "warning",
              message: `Droid stderr: ${output.trim()}`,
            }),
      ),
      Effect.catch(() => Effect.void),
      Effect.forkIn(runtimeScope),
    );

    const processExitError = (
      exit: DroidProcessExit,
      method: string,
      requestId?: string,
    ): DroidRpcError =>
      new DroidRpcError({
        kind: "process-exit",
        message:
          requestId === undefined
            ? `Cannot start Droid request ${method}: ${exit.description}`
            : `Droid process exited while ${method} was pending`,
        method,
        ...(requestId === undefined ? {} : { requestId }),
        data: exit,
      });

    const beginProcessExit = (exit: DroidProcessExit) =>
      SynchronizedRef.modify(lifecycle, (state) => {
        if (state._tag === "Exited") {
          return [false, state] as const;
        }
        return [
          true,
          {
            _tag: "ShuttingDown",
            pending: state.pending,
            exit,
          },
        ] as const;
      }).pipe(
        Effect.flatMap((transitioned) => (transitioned ? Queue.end(outgoing) : Effect.void)),
        Effect.asVoid,
      );

    const finishProcessExit = (exit: DroidProcessExit) =>
      SynchronizedRef.modify(lifecycle, (state) => {
        if (state._tag === "Exited") {
          return [undefined, state] as const;
        }
        const pending = Array.from(state.pending.entries()).filter(
          (entry): entry is [string, PendingRequest] => entry[1]._tag === "Pending",
        );
        return [pending, { _tag: "Exited", exit }] as const;
      }).pipe(
        Effect.flatMap((pending) =>
          pending === undefined
            ? Effect.void
            : Effect.forEach(
                pending,
                ([requestId, request]) =>
                  Deferred.fail(
                    request.deferred,
                    processExitError(exit, request.method, requestId),
                  ),
                { discard: true },
              ),
        ),
      );

    yield* child.exitCode.pipe(
      Effect.match({
        onFailure: (cause) =>
          ({
            code: null,
            description: `Droid process exit status was unavailable: ${String(cause)}`,
          }) satisfies DroidProcessExit,
        onSuccess: (code) =>
          ({
            code: Number(code),
            description: `Droid process exited with code ${Number(code)}`,
          }) satisfies DroidProcessExit,
      }),
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          yield* beginProcessExit(exit);
          yield* Fiber.await(stdoutFiber);
          yield* Fiber.await(stderrFiber);
          yield* finishProcessExit(exit);
          yield* Effect.all(
            [
              Queue.end(notificationPubSub),
              Queue.end(serverRequestPubSub),
              Queue.end(diagnosticPubSub),
            ],
            { discard: true },
          );
          yield* Deferred.succeed(exitDeferred, exit);
        }),
      ),
      Effect.forkIn(runtimeScope),
    );

    const request: DroidRpcClient["request"] = (method, params, options) =>
      Effect.gen(function* () {
        const requestId = String(yield* Ref.updateAndGet(nextRequestId, (id) => id + 1));
        const deferred = yield* Deferred.make<unknown, DroidRpcError>();
        yield* SynchronizedRef.modifyEffect(lifecycle, (state) => {
          if (state._tag === "Running") {
            const next = new Map(state.pending);
            next.set(requestId, { _tag: "Pending", method, deferred });
            return Effect.succeed([undefined, { ...state, pending: next }] as const);
          }
          const exit =
            state._tag === "Exited"
              ? state.exit
              : (state.exit ??
                ({
                  code: null,
                  description: "Droid process is shutting down",
                } satisfies DroidProcessExit));
          return Effect.fail(processExitError(exit, method));
        });
        const timeoutMs = options === undefined ? defaultRequestTimeoutMs : options.timeoutMs;
        const result =
          timeoutMs === undefined
            ? Deferred.await(deferred)
            : Deferred.await(deferred).pipe(
                Effect.timeoutOption(Duration.millis(timeoutMs)),
                Effect.flatMap((result) => {
                  if (Option.isSome(result)) {
                    return Effect.succeed(result.value);
                  }
                  return SynchronizedRef.modify(lifecycle, (state) => {
                    if (state._tag === "Exited") {
                      return [false, state] as const;
                    }
                    const entry = state.pending.get(requestId);
                    if (
                      entry === undefined ||
                      entry._tag !== "Pending" ||
                      entry.deferred !== deferred
                    ) {
                      return [false, state] as const;
                    }
                    const next = new Map(state.pending);
                    next.set(requestId, { _tag: "TimedOut", method });
                    return [true, { ...state, pending: next }] as const;
                  }).pipe(
                    Effect.flatMap((markedTimedOut) =>
                      markedTimedOut
                        ? Effect.fail(
                            new DroidRpcError({
                              kind: "timeout",
                              message: `Droid request ${method} timed out after ${timeoutMs}ms`,
                              method,
                              requestId,
                            }),
                          )
                        : Deferred.await(deferred),
                    ),
                  );
                }),
              );
        return yield* writeEnvelope({
          jsonrpc: "2.0",
          type: "request",
          factoryApiVersion: "1.0.0",
          id: requestId,
          method,
          params,
        }).pipe(
          Effect.andThen(result),
          Effect.ensuring(
            SynchronizedRef.update(lifecycle, (state) => {
              if (state._tag === "Exited") {
                return state;
              }
              const entry = state.pending.get(requestId);
              if (entry === undefined || entry._tag !== "Pending" || entry.deferred !== deferred) {
                return state;
              }
              const next = new Map(state.pending);
              next.delete(requestId);
              return { ...state, pending: next };
            }),
          ),
        );
      });

    const exits = Deferred.await(exitDeferred);

    const shutdown = SynchronizedRef.modifyEffect(lifecycle, (state) => {
      if (state._tag !== "Running") {
        return Effect.succeed([undefined, state] as const);
      }
      return Queue.end(outgoing).pipe(
        Effect.as([
          undefined,
          {
            _tag: "ShuttingDown",
            pending: state.pending,
          },
        ] as const),
      );
    }).pipe(
      Effect.andThen(
        Effect.raceFirst(
          exits.pipe(Effect.as(true)),
          Effect.sleep(gracefulShutdownTimeout).pipe(Effect.as(false)),
        ),
      ),
      Effect.flatMap((exited) =>
        exited
          ? Effect.void
          : child
              .kill({ killSignal: "SIGTERM", forceKillAfter: Duration.seconds(2) })
              .pipe(Effect.ignore),
      ),
    );

    yield* Effect.addFinalizer(() => shutdown);

    return {
      request,
      notifications: Stream.fromQueue(notificationPubSub),
      serverRequests: Stream.fromQueue(serverRequestPubSub),
      diagnostics: Stream.fromQueue(diagnosticPubSub),
      exits,
      shutdown,
    } satisfies DroidRpcClient;
  });
