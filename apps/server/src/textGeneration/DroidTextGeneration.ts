import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { type DroidSettings, type ModelSelection, TextGenerationError } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

import { makeDroidRpcClient } from "../provider/droid/DroidRpcClient.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const DROID_TIMEOUT_MS = 180_000;
const SESSION_INIT_TIMEOUT_MS = 75_000;
const MAX_OUTPUT_CHARS = 256 * 1024;

const isTextGenerationError = Schema.is(TextGenerationError);

export const makeDroidTextGeneration = Effect.fn("makeDroidTextGeneration")(function* (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const runDroidJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const failWith = (detail: string, cause?: unknown) =>
        new TextGenerationError({
          operation,
          detail,
          ...(cause !== undefined ? { cause } : {}),
        });
      const rpc = yield* makeDroidRpcClient({
        command: droidSettings.binaryPath,
        args: ["exec", "--input-format", "stream-jsonrpc", "--output-format", "stream-jsonrpc"],
        cwd,
        env: environment,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, commandSpawner),
        Effect.mapError((cause) => failWith("Failed to start the Droid CLI.", cause)),
      );

      const outputChunks: string[] = [];
      let outputLength = 0;
      const turnDone = yield* Deferred.make<string | undefined, TextGenerationError>();

      // Collect assistant text and resolve on turn completion. The session is
      // private to this request and carries exactly one user message, so the
      // first completed turn is ours.
      yield* Stream.runDrain(
        Stream.mapEffect(rpc.notifications, ({ notification }) => {
          switch (notification.type) {
            case "assistant_text_delta":
              return Effect.sync(() => {
                const nextLength = outputLength + notification.textDelta.length;
                if (nextLength > MAX_OUTPUT_CHARS) {
                  return false;
                }
                outputChunks.push(notification.textDelta);
                outputLength = nextLength;
                return true;
              }).pipe(
                Effect.flatMap((accepted) =>
                  accepted
                    ? Effect.void
                    : Deferred.fail(
                        turnDone,
                        failWith(`Droid output exceeded the ${MAX_OUTPUT_CHARS}-character limit.`),
                      ).pipe(Effect.asVoid),
                ),
              );
            case "agent_turn_completed":
              return Deferred.succeed(turnDone, notification.reason).pipe(Effect.asVoid);
            default:
              return Effect.void;
          }
        }),
      ).pipe(Effect.forkScoped);

      const reasoningEffort = getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");
      yield* rpc
        .request(
          "droid.initialize_session",
          {
            machineId: "default",
            cwd,
            autonomyLevel: "off",
            interactionMode: "auto",
            // Text generation must never touch the workspace.
            restrictToolIds: [],
            ...(modelSelection.model ? { modelId: modelSelection.model } : {}),
            ...(reasoningEffort ? { reasoningEffort } : {}),
          },
          { timeoutMs: SESSION_INIT_TIMEOUT_MS },
        )
        .pipe(Effect.mapError((cause) => failWith("Failed to initialize Droid session.", cause)));

      yield* rpc
        .request("droid.add_user_message", { text: prompt })
        .pipe(Effect.mapError((cause) => failWith("Droid rejected the prompt.", cause)));

      // Race the completion against process death: a crashed CLI must fail
      // immediately, not ride out the full generation timeout.
      const completionReason = yield* Effect.raceFirst(
        Deferred.await(turnDone),
        Effect.flatMap(rpc.exits, (exit) =>
          Effect.fail(
            failWith(`Droid exited before completing the request (${exit.description}).`),
          ),
        ),
      ).pipe(
        Effect.timeoutOption(DROID_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(failWith("Droid request timed out.")),
            onSome: (value) => Effect.succeed(value),
          }),
        ),
      );

      const trimmed = outputChunks.join("").trim();
      if (!trimmed) {
        return yield* failWith(
          completionReason === "cancelled"
            ? "Droid request was cancelled."
            : "Droid returned empty output.",
        );
      }

      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));
      return yield* decodeOutput(extractJsonObject(trimmed)).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(failWith("Droid returned invalid structured output.", cause)),
        }),
      );
    }).pipe(
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({
              operation,
              detail: "Droid text generation failed.",
              cause,
            }),
      ),
      Effect.scoped,
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("DroidTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runDroidJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("DroidTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runDroidJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("DroidTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runDroidJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("DroidTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runDroidJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
