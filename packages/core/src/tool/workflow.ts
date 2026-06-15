export * as WorkflowTool from "./workflow"

import { ToolFailure } from "@opencode-ai/llm"
import { Effect, Layer, Schema } from "effect"
import { PermissionV2 } from "../permission"
import { NonNegativeInt, PositiveInt } from "../schema"
import { SessionSchema } from "../session/schema"
import { WorkflowRuntime } from "../workflow"
import { Tool } from "./tool"
import { Tools } from "./tools"

export const name = "workflow"

const RunID = Schema.String.check(Schema.isPattern(/^wf_[0-9A-Za-z]+$/)).annotate({
  description: "Workflow run id, for example wf_019b1c0ffeeABCDEF01234567.",
})

const SessionID = SessionSchema.ID.pipe(Schema.optional).annotate({
  description: "Session id to act on. Defaults to the current session.",
})

const RunOperation = Schema.Struct({
  action: Schema.Literal("run"),
  script: Schema.String.annotate({
    description:
      "Inline JavaScript body executed in the workflow sandbox. Available globals: args, workspace, agent, parallel, pipeline, phase, log, readFile, writeFile, exists, glob.",
  }),
  name: Schema.String.pipe(Schema.optional).annotate({ description: "Optional display name for the workflow run." }),
  args: Schema.Unknown.pipe(Schema.optional).annotate({ description: "JSON value exposed to the script as args." }),
  workspace: Schema.String.pipe(Schema.optional).annotate({
    description: "Workspace directory recorded for the run. Defaults to the current location directory.",
  }),
  worktree: Schema.Boolean.pipe(Schema.optional).annotate({
    description: "Create a managed git worktree for this workflow run when the host installs a workflow workspace hook.",
  }),
  removeWorktreeOnFinish: Schema.Boolean.pipe(Schema.optional).annotate({
    description: "Remove the managed worktree after completion or failure. Default false.",
  }),
  removeWorktreeOnCancel: Schema.Boolean.pipe(Schema.optional).annotate({
    description: "Remove the managed worktree after cancel or reclaim. Default true.",
  }),
  maxConcurrentAgents: PositiveInt.pipe(Schema.optional).annotate({
    description: "Maximum concurrent agent() calls. Defaults to the runtime setting.",
  }),
  scriptTimeoutMs: PositiveInt.pipe(Schema.optional).annotate({
    description: "Workflow script wall-clock timeout in milliseconds.",
  }),
  sessionID: SessionID,
})

const ListOperation = Schema.Struct({
  action: Schema.Literal("list"),
  sessionID: SessionID,
})

const StatusOperation = Schema.Struct({
  action: Schema.Literal("status"),
  runID: RunID,
})

const WaitOperation = Schema.Struct({
  action: Schema.Literal("wait"),
  runID: RunID,
  timeoutMs: NonNegativeInt.pipe(Schema.optional).annotate({
    description: "Maximum wait in milliseconds. Omit to wait until completion.",
  }),
})

const CancelOperation = Schema.Struct({
  action: Schema.Literal("cancel"),
  runID: RunID,
})

const ResumeOperation = Schema.Struct({
  action: Schema.Literal("resume"),
  runID: RunID,
  maxConcurrentAgents: PositiveInt.pipe(Schema.optional).annotate({
    description: "Maximum concurrent agent() calls for the resumed run.",
  }),
  scriptTimeoutMs: PositiveInt.pipe(Schema.optional).annotate({
    description: "Workflow script wall-clock timeout in milliseconds for the resumed run.",
  }),
})

const ReclaimOperation = Schema.Struct({
  action: Schema.Literal("reclaim"),
  runID: RunID,
})

const Operation = Schema.Union([
  RunOperation,
  ListOperation,
  StatusOperation,
  WaitOperation,
  CancelOperation,
  ResumeOperation,
  ReclaimOperation,
]).annotate({ description: "Workflow runtime operation." })

const RunOutcome = Schema.Union([
  Schema.Struct({ status: Schema.Literal("completed"), result: Schema.Unknown }),
  Schema.Struct({ status: Schema.Literal("failed"), error: Schema.String }),
  Schema.Struct({ status: Schema.Literal("cancelled") }),
])

export const Input = Schema.Struct({
  operation: Operation,
})

export const Output = Schema.Struct({
  message: Schema.String,
  run: WorkflowRuntime.Summary.pipe(Schema.optional),
  runs: Schema.Array(WorkflowRuntime.Summary).pipe(Schema.optional),
  outcome: RunOutcome.pipe(Schema.optional),
  resumed: Schema.Boolean.pipe(Schema.optional),
})
export type Output = typeof Output.Type

export const toModelOutput = (output: typeof Output.Encoded) =>
  [
    output.message,
    output.run ? `\nRun:\n${JSON.stringify(output.run, null, 2)}` : "",
    output.runs ? `\nRuns:\n${JSON.stringify(output.runs, null, 2)}` : "",
    output.outcome ? `\nOutcome:\n${JSON.stringify(output.outcome, null, 2)}` : "",
  ].join("")

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const tools = yield* Tools.Service
    const workflows = yield* WorkflowRuntime.Service
    const permission = yield* PermissionV2.Service

    yield* tools
      .register({
        [name]: Tool.make({
          description: [
            "Run durable JavaScript workflows in a restricted sandbox with journaled resume, cancellation, reclaim, and bounded agent fan-out.",
            "Use action=run for inline workflow scripts, then action=status or action=wait to inspect completion.",
            "Inside scripts, use args, workspace, phase(title), log(message), readFile(path), writeFile(path, content), exists(path), glob(pattern), parallel(thunks), pipeline(items, ...stages), and agent(prompt, options).",
            "agent() resolves through the workflow agent hook when one is installed; without that hook the workflow fails and the call is counted as failed.",
            "Set worktree=true to request a managed git worktree when the host installs a workflow workspace hook.",
          ].join(" "),
          input: Input,
          output: Output,
          toModelOutput: ({ output }) => [{ type: "text", text: toModelOutput(output) }],
          execute: (input, context) =>
            Effect.gen(function* () {
              yield* permission.assert({
                action: name,
                resources: ["*"],
                save: ["*"],
                sessionID: context.sessionID,
                agent: context.agent,
                source: { type: "tool", messageID: context.assistantMessageID, callID: context.toolCallID },
              })
              const operation = input.operation
              if (operation.action === "run") {
                const run = yield* workflows.start({
                  sessionID: operation.sessionID ?? context.sessionID,
                  name: operation.name,
                  script: operation.script,
                  args: operation.args,
                  workspace: operation.workspace,
                  worktree: operation.worktree
                    ? {
                        removeOnFinish: operation.removeWorktreeOnFinish,
                        removeOnCancel: operation.removeWorktreeOnCancel,
                      }
                    : undefined,
                  maxConcurrentAgents: operation.maxConcurrentAgents,
                  scriptTimeoutMs: operation.scriptTimeoutMs,
                })
                return { message: `Started workflow ${run.runID}.`, run }
              }
              if (operation.action === "list") {
                const runs = yield* workflows.list({ sessionID: operation.sessionID ?? context.sessionID })
                return { message: `Found ${runs.length} workflow run${runs.length === 1 ? "" : "s"}.`, runs }
              }
              if (operation.action === "status") {
                const run = yield* workflows.status(operation.runID)
                return run ? { message: `Loaded workflow ${run.runID}.`, run } : { message: `Workflow ${operation.runID} not found.` }
              }
              if (operation.action === "wait") {
                const outcome = yield* workflows.wait({ runID: operation.runID, timeoutMs: operation.timeoutMs })
                return outcome
                  ? { message: `Workflow ${operation.runID} ${outcome.status}.`, outcome }
                  : { message: `Workflow ${operation.runID} is still running or unknown.` }
              }
              if (operation.action === "cancel") {
                const run = yield* workflows.cancel(operation.runID)
                return run
                  ? { message: `Cancelled workflow ${operation.runID}.`, run }
                  : { message: `Workflow ${operation.runID} not found.` }
              }
              if (operation.action === "resume") {
                const result = yield* workflows.resume({
                  runID: operation.runID,
                  maxConcurrentAgents: operation.maxConcurrentAgents,
                  scriptTimeoutMs: operation.scriptTimeoutMs,
                })
                return {
                  message: result.resumed
                    ? `Resumed workflow ${result.runID}.`
                    : `Workflow ${result.runID} was not resumable.`,
                  resumed: result.resumed,
                }
              }
              const run = yield* workflows.reclaim(operation.runID)
              return run
                ? { message: `Reclaimed workflow ${operation.runID}.`, run }
                : { message: `Workflow ${operation.runID} not found.` }
            }).pipe(
              Effect.mapError((error) =>
                error instanceof ToolFailure ? error : new ToolFailure({ message: "Unable to use workflow runtime" }),
              ),
            ),
        }),
      })
      .pipe(Effect.orDie)
  }),
)
