import * as Cause from "effect/Cause";
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
const timedOutRequestRetentionLimit = 256;
const diagnosticTextLimit = 2000;
const outgoingQueueCapacity = 2;
const notificationQueueCapacity = 64;
const lossyNotificationQueueLimit = notificationQueueCapacity - 1;
export const DROID_SERVER_REQUEST_CONCURRENCY = 16;
const serverRequestQueueCapacity = DROID_SERVER_REQUEST_CONCURRENCY;
const maxOutgoingMessageBytes = 128 * 1024 * 1024;
const maxIncomingLineBytes = 2 * 1024 * 1024;
const stdoutExitObservationGrace = Duration.millis(100);
const lossyNotificationTypes: ReadonlySet<string> = new Set(["tool_progress_update"]);

export interface DroidRpcSpawnInput {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd?: string;
  readonly env?: NodeJS.ProcessEnv;
}

export interface DroidExecRpcInput {
  readonly binaryPath: string;
  readonly cwd?: string;
  readonly env?: NodeJS.ProcessEnv;
}

export interface DroidProcessExit {
  readonly code: number | null;
  readonly signal?: string;
  readonly description: string;
}

export class DroidRpcSpawnError extends Schema.TaggedErrorClass<DroidRpcSpawnError>()(
  "DroidRpcSpawnError",
  {
    command: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return `Failed to spawn Droid process for command: ${this.command}`;
  }
}

export class DroidRpcError extends Schema.TaggedErrorClass<DroidRpcError>()("DroidRpcError", {
  kind: Schema.Literals([
    "encode",
    "write",
    "timeout",
    "rpc",
    "process-exit",
    "duplicate-response",
    "duplicate-server-response",
    "message-too-large",
    "frame-too-large",
  ]),
  method: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
  code: Schema.optionalKey(Schema.Number),
  data: Schema.optionalKey(Schema.Unknown),
  cause: Schema.optionalKey(Schema.Defect()),
  rpcMessage: Schema.optionalKey(Schema.String),
  timeoutMs: Schema.optionalKey(Schema.Number),
  exitDescription: Schema.optionalKey(Schema.String),
  actualBytes: Schema.optionalKey(Schema.Number),
  limitBytes: Schema.optionalKey(Schema.Number),
}) {
  override get message() {
    switch (this.kind) {
      case "encode":
        return "Failed to encode Droid JSON-RPC message";
      case "write":
        return "Failed to write to Droid process stdin because it is closed";
      case "timeout":
        return `Droid request ${this.method} timed out after ${this.timeoutMs}ms`;
      case "rpc":
        return this.rpcMessage ?? "Droid returned an invalid JSON-RPC error response";
      case "process-exit":
        return this.requestId === undefined
          ? `Cannot start Droid request ${this.method}: ${this.exitDescription}`
          : `Droid process exited while ${this.method} was pending`;
      case "duplicate-response":
        return `Droid request ${this.method} responded after timing out`;
      case "duplicate-server-response":
        return `Droid server request ${this.method} was answered more than once`;
      case "message-too-large":
        return `Droid JSON-RPC message is ${this.actualBytes} bytes; limit is ${this.limitBytes}`;
      case "frame-too-large":
        return `Droid JSON-RPC line exceeded the ${this.limitBytes} byte limit`;
    }
  }
}

interface DroidServerRequestBase {
  readonly id: string | number;
  readonly sessionId: string | undefined;
  readonly respond: (result: unknown) => Effect.Effect<void, DroidRpcError>;
  readonly fail: (code: number, message: string) => Effect.Effect<void, DroidRpcError>;
}

export interface DroidPermissionServerRequest extends DroidServerRequestBase {
  readonly method: "droid.request_permission";
  readonly params: DroidPermissionRequestType;
  readonly rawParams: unknown;
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
  readonly exits: Effect.Effect<DroidProcessExit>;
  readonly shutdown: Effect.Effect<void>;
}

interface ParsedJsonRpcMessage {
  readonly jsonrpc: "2.0";
  readonly type: "request" | "response" | "notification";
  readonly id?: string | number | null;
  readonly method?: string;
  readonly params?: unknown;
  readonly result?: unknown;
  readonly error?: unknown;
  readonly [key: string]: unknown;
}

type ParseJsonRpcLineResult =
  | { readonly _tag: "Message"; readonly message: ParsedJsonRpcMessage }
  | { readonly _tag: "Invalid"; readonly error: string };

function parseJsonRpcLine(line: string): ParseJsonRpcLineResult {
  try {
    const parsed: unknown = JSON.parse(line);
    if (!Predicate.isObject(parsed) || Array.isArray(parsed)) {
      return { _tag: "Invalid", error: "JSON-RPC line must contain an object" };
    }
    if (parsed.jsonrpc !== "2.0") {
      return { _tag: "Invalid", error: 'JSON-RPC line must include jsonrpc: "2.0"' };
    }
    if (parsed.type !== "request" && parsed.type !== "response" && parsed.type !== "notification") {
      return {
        _tag: "Invalid",
        error: "JSON-RPC line must include a valid type discriminator",
      };
    }
    if (
      "id" in parsed &&
      parsed.id !== null &&
      typeof parsed.id !== "string" &&
      typeof parsed.id !== "number"
    ) {
      return {
        _tag: "Invalid",
        error: "JSON-RPC id must be a string, number, null, or absent",
      };
    }
    return {
      _tag: "Message",
      message: {
        ...parsed,
        jsonrpc: "2.0",
        type: parsed.type,
      },
    };
  } catch (cause) {
    return {
      _tag: "Invalid",
      error: cause instanceof Error ? cause.message : String(cause),
    };
  }
}

interface PendingRequest {
  readonly _tag: "Pending";
  readonly method: string;
  readonly deferred: Deferred.Deferred<unknown, DroidRpcError>;
  readonly sent: boolean;
}

interface TimedOutRequest {
  readonly _tag: "TimedOut";
  readonly method: string;
}

type RequestState = PendingRequest | TimedOutRequest;

interface LineFramingState {
  readonly buffer: string;
  readonly bytes: number;
}

function splitJsonRpcLines(
  stream: Stream.Stream<string, unknown>,
): Stream.Stream<string, unknown | DroidRpcError> {
  return stream.pipe(
    Stream.mapAccumEffect(
      (): LineFramingState => ({ buffer: "", bytes: 0 }),
      (state, chunk) =>
        Effect.gen(function* () {
          const lines: string[] = [];
          let buffer = state.buffer;
          let bytes = state.bytes;
          let cursor = 0;
          const delimiters = /\r\n|\r|\n/g;
          for (const delimiter of chunk.matchAll(delimiters)) {
            const index = delimiter.index;
            const segment = chunk.slice(cursor, index);
            buffer += segment;
            bytes += Buffer.byteLength(segment, "utf8");
            if (bytes > maxIncomingLineBytes) {
              return yield* new DroidRpcError({
                kind: "frame-too-large",
                actualBytes: bytes,
                limitBytes: maxIncomingLineBytes,
              });
            }
            lines.push(buffer);
            buffer = "";
            bytes = 0;
            cursor = index + delimiter[0].length;
          }
          const remainder = chunk.slice(cursor);
          buffer += remainder;
          bytes += Buffer.byteLength(remainder, "utf8");
          if (bytes > maxIncomingLineBytes) {
            return yield* new DroidRpcError({
              kind: "frame-too-large",
              actualBytes: bytes,
              limitBytes: maxIncomingLineBytes,
            });
          }
          return [{ buffer, bytes }, lines] as const;
        }),
      {
        onHalt: (state) => (state.buffer.length > 0 ? [state.buffer] : []),
      },
    ),
  );
}

function markRequestTimedOut(
  pending: ReadonlyMap<string, RequestState>,
  requestId: string,
  method: string,
): ReadonlyMap<string, RequestState> {
  const next = new Map(pending);
  next.delete(requestId);
  next.set(requestId, { _tag: "TimedOut", method });

  let timedOutCount = 0;
  for (const request of next.values()) {
    if (request._tag === "TimedOut") {
      timedOutCount += 1;
    }
  }
  if (timedOutCount <= timedOutRequestRetentionLimit) {
    return next;
  }

  for (const [retainedRequestId, request] of next) {
    if (request._tag !== "TimedOut") {
      continue;
    }
    next.delete(retainedRequestId);
    timedOutCount -= 1;
    if (timedOutCount <= timedOutRequestRetentionLimit) {
      break;
    }
  }
  return next;
}

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
      rpcMessage: error.message,
      method,
      requestId,
      ...(typeof error.code === "number" ? { code: error.code } : {}),
      ...("data" in error ? { data: error.data } : {}),
    });
  }
  return new DroidRpcError({
    kind: "rpc",
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
    const outgoing = yield* Queue.bounded<string, Cause.Done<void>>(outgoingQueueCapacity);
    const notificationPubSub = yield* Queue.bounded<DroidNotificationEnvelope, Cause.Done<void>>(
      notificationQueueCapacity,
    );
    const serverRequestPubSub = yield* Queue.bounded<DroidServerRequest, Cause.Done<void>>(
      serverRequestQueueCapacity,
    );
    const lifecycle = yield* SynchronizedRef.make<DroidRpcLifecycle>({
      _tag: "Running",
      pending: new Map(),
    });
    const nextRequestId = yield* Ref.make(0);
    const exitDeferred = yield* Deferred.make<DroidProcessExit>();
    const droppedLossyNotificationCount = yield* Ref.make(0);

    const publishDiagnostic = (
      message: string,
      options?: {
        readonly line?: string;
        readonly cause?: unknown;
      },
    ) =>
      Effect.logWarning(message.slice(0, diagnosticTextLimit), {
        ...(options?.line === undefined
          ? {}
          : { line: options.line.slice(0, diagnosticTextLimit) }),
        ...(options?.cause === undefined
          ? {}
          : { cause: String(options.cause).slice(0, diagnosticTextLimit) }),
      });

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
              cause,
            }),
        ),
        Effect.flatMap((encoded) => {
          const actualBytes = Buffer.byteLength(encoded, "utf8");
          return actualBytes <= maxOutgoingMessageBytes
            ? Effect.succeed(encoded)
            : Effect.fail(
                new DroidRpcError({
                  kind: "message-too-large",
                  actualBytes,
                  limitBytes: maxOutgoingMessageBytes,
                }),
              );
        }),
        Effect.flatMap((encoded) => Queue.offer(outgoing, encoded)),
        Effect.flatMap((offered) =>
          offered
            ? Effect.void
            : Effect.fail(
                new DroidRpcError({
                  kind: "write",
                }),
              ),
        ),
      );

    const sendResponse = (
      id: string | number,
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
        if (typeof message.id !== "string") {
          yield* publishDiagnostic("Ignoring Droid JSON-RPC response without a string id");
          return;
        }
        const requestId = message.id;
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
          yield* publishDiagnostic(`Ignoring response for unknown Droid request ${requestId}`);
          return;
        }
        if (requestState._tag === "TimedOut") {
          const error = new DroidRpcError({
            kind: "duplicate-response",
            method: requestState.method,
            requestId,
          });
          yield* publishDiagnostic(error.message, { cause: error });
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

    const makeServerRequestBase = (
      id: string | number,
      method: string,
      sessionId: string | undefined,
    ) =>
      Effect.gen(function* () {
        const answered = yield* SynchronizedRef.make(false);
        const requestId = String(id);
        const answerOnce = (
          result:
            | { readonly _tag: "Success"; readonly value: unknown }
            | { readonly _tag: "Failure"; readonly code: number; readonly message: string },
        ) =>
          SynchronizedRef.modifyEffect(answered, (alreadyAnswered) =>
            alreadyAnswered
              ? Effect.fail(
                  new DroidRpcError({
                    kind: "duplicate-server-response",
                    method,
                    requestId,
                  }),
                )
              : sendResponse(id, result).pipe(Effect.as([undefined, true] as const)),
          );
        return {
          id,
          sessionId,
          respond: (result: unknown) =>
            answerOnce({
              _tag: "Success",
              value: result,
            }),
          fail: (code: number, message: string) =>
            answerOnce({
              _tag: "Failure",
              code,
              message,
            }),
        };
      });

    const publishServerRequest = (request: DroidServerRequest) =>
      Queue.offer(serverRequestPubSub, request).pipe(Effect.asVoid);

    const handleServerRequest = (
      message: ParsedJsonRpcMessage,
    ): Effect.Effect<void, DroidRpcError> =>
      Effect.gen(function* () {
        if (message.id === undefined || message.id === null || typeof message.method !== "string") {
          yield* publishDiagnostic("Ignoring malformed server-initiated Droid request");
          return;
        }
        const id = message.id;
        const sessionId =
          Predicate.isObject(message.params) && typeof message.params.sessionId === "string"
            ? message.params.sessionId
            : undefined;
        if (message.method === "droid.request_permission") {
          const decoded = yield* decodePermissionRequest(message.params).pipe(Effect.result);
          if (decoded._tag === "Failure") {
            yield* publishDiagnostic("Unable to decode droid.request_permission params", {
              cause: decoded.failure,
            });
            yield* sendResponse(id, {
              _tag: "Failure",
              code: -32602,
              message: "Invalid droid.request_permission params",
            }).pipe(Effect.ignore);
            return;
          }
          const requestBase = yield* makeServerRequestBase(id, message.method, sessionId);
          yield* publishServerRequest({
            ...requestBase,
            method: message.method,
            params: decoded.success,
            rawParams: message.params,
          });
          return;
        }
        if (message.method === "droid.ask_user") {
          const decoded = yield* decodeAskUserRequest(message.params).pipe(Effect.result);
          if (decoded._tag === "Failure") {
            yield* publishDiagnostic("Unable to decode droid.ask_user params", {
              cause: decoded.failure,
            });
            yield* sendResponse(id, {
              _tag: "Failure",
              code: -32602,
              message: "Invalid droid.ask_user params",
            }).pipe(Effect.ignore);
            return;
          }
          const requestBase = yield* makeServerRequestBase(id, message.method, sessionId);
          yield* publishServerRequest({
            ...requestBase,
            method: message.method,
            params: decoded.success,
          });
          return;
        }
        yield* publishDiagnostic(
          `Ignoring unsupported server-initiated Droid request ${message.method}`,
        );
        yield* sendResponse(id, {
          _tag: "Failure",
          code: -32601,
          message: `Unsupported Droid request: ${message.method}`,
        }).pipe(Effect.ignore);
      });

    const handleNotification = (
      message: ParsedJsonRpcMessage,
    ): Effect.Effect<void, DroidRpcError> =>
      Effect.gen(function* () {
        if (message.method !== "droid.session_notification") {
          return;
        }
        if (!Predicate.isObject(message.params)) {
          yield* publishDiagnostic("Ignoring Droid session notification with invalid params");
          return;
        }
        const decoded = yield* decodeNotification(message.params.notification).pipe(Effect.result);
        if (decoded._tag === "Failure") {
          yield* publishDiagnostic("Unable to decode Droid session notification", {
            cause: decoded.failure,
          });
          return;
        }
        const sessionId =
          typeof message.params.sessionId === "string" ? message.params.sessionId : undefined;
        const notification = decoded.success;
        if (
          lossyNotificationTypes.has(notification.type) &&
          Queue.sizeUnsafe(notificationPubSub) >= lossyNotificationQueueLimit
        ) {
          const droppedCount = yield* Ref.updateAndGet(
            droppedLossyNotificationCount,
            (count) => count + 1,
          );
          if (droppedCount === 1 || droppedCount % notificationQueueCapacity === 0) {
            yield* publishDiagnostic(
              `Dropped ${droppedCount} lossy Droid session notifications because the delivery queue is saturated`,
            );
          }
          return;
        }
        yield* Queue.offer(notificationPubSub, {
          sessionId,
          notification,
        });
      });

    const handleMessage = (message: ParsedJsonRpcMessage): Effect.Effect<void, DroidRpcError> => {
      switch (message.type) {
        case "request":
          return handleServerRequest(message);
        case "notification":
          return handleNotification(message);
        case "response":
          return resolveResponse(message);
      }
    };

    const handleLine = (line: string) => {
      const parsed = parseJsonRpcLine(line);
      return parsed._tag === "Invalid"
        ? publishDiagnostic(`Unable to parse Droid JSON-RPC line: ${parsed.error}`, { line })
        : handleMessage(parsed.message);
    };

    const processExitError = (
      exit: DroidProcessExit,
      method: string,
      requestId?: string,
    ): DroidRpcError =>
      new DroidRpcError({
        kind: "process-exit",
        method,
        ...(requestId === undefined ? {} : { requestId }),
        ...(requestId === undefined ? { exitDescription: exit.description } : {}),
        data: exit,
      });

    const observeChildExit = yield* Effect.cached(
      child.exitCode.pipe(
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
      ),
    );

    const beginProcessExit = (exit: DroidProcessExit) =>
      SynchronizedRef.modify(lifecycle, (state) => {
        if (state._tag !== "Running") {
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
        Effect.flatMap((transitioned) =>
          (transitioned ? Queue.end(outgoing) : Effect.void).pipe(Effect.as(transitioned)),
        ),
      );

    const finishProcessExit = (exit: DroidProcessExit) =>
      SynchronizedRef.modify(lifecycle, (state) => {
        if (state._tag === "Exited") {
          return [undefined, state] as const;
        }
        const finalExit =
          state._tag === "ShuttingDown" && state.exit !== undefined ? state.exit : exit;
        const pending = Array.from(state.pending.entries()).filter(
          (entry): entry is [string, PendingRequest] => entry[1]._tag === "Pending",
        );
        return [
          { finalExit, pending },
          { _tag: "Exited", exit: finalExit },
        ] as const;
      }).pipe(
        Effect.flatMap((transition) =>
          transition === undefined
            ? Effect.void
            : Effect.forEach(
                transition.pending,
                ([requestId, request]) =>
                  Deferred.fail(
                    request.deferred,
                    processExitError(transition.finalExit, request.method, requestId),
                  ),
                { discard: true },
              ).pipe(Effect.as(transition.finalExit)),
        ),
      );

    const finalizeTransportExit = (exit: DroidProcessExit) =>
      Effect.gen(function* () {
        yield* Queue.end(outgoing);
        const finalExit = yield* finishProcessExit(exit);
        if (finalExit === undefined) return;
        yield* Effect.all([Queue.end(notificationPubSub), Queue.end(serverRequestPubSub)], {
          discard: true,
        });
        yield* Deferred.succeed(exitDeferred, finalExit);
      });

    const terminateTransport = (exit: DroidProcessExit) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (!(yield* beginProcessExit(exit))) {
            return;
          }
          yield* child
            .kill({ killSignal: "SIGTERM", forceKillAfter: Duration.seconds(2) })
            .pipe(Effect.ignore);
          yield* observeChildExit;
          yield* finalizeTransportExit(exit);
        }),
      );

    const stdoutFiber = yield* child.stdout.pipe(
      Stream.decodeText(),
      splitJsonRpcLines,
      Stream.filter((line) => line.trim().length > 0),
      Stream.runForEach(handleLine),
      Effect.matchCauseEffect({
        onFailure: (cause) =>
          publishDiagnostic("Droid stdout stream failed", { cause }).pipe(
            Effect.andThen(
              terminateTransport({
                code: null,
                description: `Droid stdout stream failed: ${Cause.pretty(cause)}`,
              }),
            ),
          ),
        onSuccess: () =>
          observeChildExit.pipe(
            Effect.timeoutOption(stdoutExitObservationGrace),
            Effect.flatMap(
              Option.match({
                onNone: () =>
                  terminateTransport({
                    code: null,
                    description: "Droid stdout stream closed before the process exited",
                  }),
                onSome: finalizeTransportExit,
              }),
            ),
          ),
      }),
      Effect.forkIn(runtimeScope),
    );

    const stderrFiber = yield* child.stderr.pipe(
      Stream.decodeText(),
      Stream.runForEach((output) =>
        output.trim().length === 0
          ? Effect.void
          : publishDiagnostic(`Droid stderr: ${output.trim()}`),
      ),
      Effect.catch(() => Effect.void),
      Effect.forkIn(runtimeScope),
    );

    yield* observeChildExit.pipe(
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          yield* beginProcessExit(exit);
          yield* Fiber.await(stdoutFiber);
          yield* Fiber.await(stderrFiber);
          yield* finalizeTransportExit(exit);
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
            next.set(requestId, { _tag: "Pending", method, deferred, sent: false });
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
        const exchange = writeEnvelope({
          jsonrpc: "2.0",
          type: "request",
          factoryApiVersion: "1.0.0",
          id: requestId,
          method,
          params,
        }).pipe(
          Effect.tap(() =>
            SynchronizedRef.update(lifecycle, (state) => {
              if (state._tag === "Exited") {
                return state;
              }
              const entry = state.pending.get(requestId);
              if (entry === undefined || entry._tag !== "Pending" || entry.deferred !== deferred) {
                return state;
              }
              const next = new Map(state.pending);
              next.set(requestId, { ...entry, sent: true });
              return { ...state, pending: next };
            }),
          ),
          Effect.andThen(Deferred.await(deferred)),
        );
        const result =
          timeoutMs === undefined
            ? exchange
            : exchange.pipe(
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
                    if (entry.sent) {
                      return [
                        true,
                        {
                          ...state,
                          pending: markRequestTimedOut(state.pending, requestId, method),
                        },
                      ] as const;
                    }
                    const next = new Map(state.pending);
                    next.delete(requestId);
                    return [true, { ...state, pending: next }] as const;
                  }).pipe(
                    Effect.flatMap((markedTimedOut) =>
                      markedTimedOut
                        ? Effect.fail(
                            new DroidRpcError({
                              kind: "timeout",
                              method,
                              requestId,
                              timeoutMs,
                            }),
                          )
                        : Deferred.await(deferred),
                    ),
                  );
                }),
              );
        return yield* result.pipe(
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
      exits,
      shutdown,
    } satisfies DroidRpcClient;
  });

export const makeDroidExecRpcClient = (input: DroidExecRpcInput) =>
  makeDroidRpcClient({
    command: input.binaryPath,
    args: ["exec", "--input-format", "stream-jsonrpc", "--output-format", "stream-jsonrpc"],
    ...(input.cwd === undefined ? {} : { cwd: input.cwd }),
    ...(input.env === undefined ? {} : { env: input.env }),
  });
