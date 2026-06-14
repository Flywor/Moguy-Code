import * as Tool from "./tool"
import DESCRIPTION from "./task.txt"
import { ToolJsonSchema } from "./json-schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { SessionID, MessageID } from "../session/schema"
import { MessageV2 } from "../session/message-v2"
import { Agent } from "../agent/agent"
import { deriveSubagentSessionPermission } from "../agent/subagent-permissions"
import type { SessionPrompt } from "../session/prompt"
import { Config } from "@/config/config"
import { TaskScheduler } from "./task-scheduler"
import { Effect, Exit, Schema, Scope } from "effect"
import { EffectBridge } from "@/effect/bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Database } from "@opencode-ai/core/database/database"
import { SessionMemory } from "@opencode-ai/core/session/memory"

export interface TaskPromptOps {
  cancel(sessionID: SessionID): Effect.Effect<void>
  resolvePromptParts(template: string): Effect.Effect<SessionPrompt.PromptInput["parts"]>
  prompt(input: SessionPrompt.PromptInput): Effect.Effect<SessionV1.WithParts>
}

const id = "task"
const BACKGROUND_DESCRIPTION = [
  "Background mode: background=true launches the subagent asynchronously and returns immediately.",
  "Foreground is the default; use it when you need the result before continuing.",
  "Use background only for independent work that can run while you continue elsewhere.",
  "You will be notified automatically when it finishes.",
].join(" ")
const BACKGROUND_STARTED = [
  "The task is working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.",
].join("\n")
const BACKGROUND_UPDATED = [
  "Additional context sent to the running background task.",
  "The task is still working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you sent and end your response.",
].join("\n")
const MERGED_RUNNING = [
  "This task was merged with an equivalent task that is already running.",
  "Do not duplicate the same search or edits; use the task_id above if you need to add targeted context later.",
].join("\n")

const TaskKind = Schema.Literals(["read", "write", "test", "review", "plan"])
const TaskMemoryScope = SessionMemory.Scope
const TaskScope = Schema.Struct({
  files: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "Files or globs the subagent should limit itself to",
  }),
  symbols: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "Symbols, functions, classes, or APIs the subagent should focus on",
  }),
  topics: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "Topics or subsystems the subagent should focus on",
  }),
  operations: Schema.optional(Schema.Array(TaskKind)).annotate({
    description: "The operation types covered by this scope",
  }),
})
const ModelBudget = Schema.Struct({
  maxTokens: Schema.optional(Schema.Finite).annotate({ description: "Approximate maximum model tokens to spend" }),
  maxCost: Schema.optional(Schema.Finite).annotate({ description: "Approximate maximum model cost to spend" }),
})

const BaseParameterFields = {
  description: Schema.String.annotate({ description: "A short (3-5 words) description of the task" }),
  prompt: Schema.String.annotate({ description: "The task for the agent to perform" }),
  subagent_type: Schema.String.annotate({ description: "The type of specialized agent to use for this task" }),
  task_id: Schema.optional(Schema.String).annotate({
    description:
      "This should only be set if you mean to resume a previous task (you can pass a prior task_id and the task will continue the same subagent session as before instead of creating a fresh one)",
  }),
  command: Schema.optional(Schema.String).annotate({ description: "The command that triggered this task" }),
  scope: Schema.optional(TaskScope).annotate({
    description: "Scheduler scope for the subagent: files, symbols, topics, and operation types",
  }),
  expected_output: Schema.optional(Schema.String).annotate({
    description: "The exact shape or deliverable expected from this subagent",
  }),
  timeout_ms: Schema.optional(Schema.Finite).annotate({
    description: "Maximum time in milliseconds for dependency waits and this subagent execution",
  }),
  model_budget: Schema.optional(ModelBudget).annotate({
    description: "Soft model budget for this subagent",
  }),
  depends_on: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "Task IDs that must complete before this subagent runs",
  }),
  task_kind: Schema.optional(TaskKind).annotate({
    description: "Scheduler operation kind: read, write, test, review, or plan",
  }),
  owned_files: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "Files this writing subagent owns. Required for write tasks.",
  }),
  merge_key: Schema.optional(Schema.String).annotate({
    description: "Stable key for merging equivalent tasks so multiple subagents do not repeat the same work",
  }),
  isolation: Schema.optional(Schema.Literals(["readonly", "ownership", "patch", "worktree"])).annotate({
    description: "Write isolation mode. Read/review tasks default to readonly; write tasks default to ownership.",
  }),
  memory_scope: Schema.optional(TaskMemoryScope).annotate({
    description:
      "Memory to inject into a fresh or resumed subagent: none, session, project, or session-project. Use only when the task depends on prior session/project context.",
  }),
}

const BaseParameters = Schema.Struct(BaseParameterFields)

export const Parameters = Schema.Struct({
  ...BaseParameterFields,
  background: Schema.optional(Schema.Boolean).annotate({
    description:
      "Run the agent in the background. You will be notified when it completes. DO NOT sleep, poll, or proactively check on its progress",
  }),
})

function renderOutput(input: {
  sessionID: SessionID
  state: "running" | "completed" | "error"
  summary?: string
  text: string
}) {
  const tag = input.state === "error" ? "task_error" : "task_result"
  return [
    `<task id="${input.sessionID}" state="${input.state}">`,
    ...(input.summary ? [`<summary>${input.summary}</summary>`] : []),
    `<${tag}>`,
    input.text,
    `</${tag}>`,
    "</task>",
  ].join("\n")
}

export const TaskTool = Tool.define(
  id,
  Effect.gen(function* () {
    const agent = yield* Agent.Service
    const background = yield* BackgroundJob.Service
    const config = yield* Config.Service
    const sessions = yield* Session.Service
    const scope = yield* Scope.Scope
    const flags = yield* RuntimeFlags.Service
    const database = yield* Database.Service
    const scheduler = yield* TaskScheduler.Service

    const run = Effect.fn("TaskTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      const cfg = yield* config.get()
      const runInBackground = params.background === true
      if (runInBackground && !flags.experimentalBackgroundSubagents) {
        return yield* Effect.fail(
          new Error("Background subagents require OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true"),
        )
      }

      if (!ctx.extra?.bypassAgentCheck) {
        yield* ctx.ask({
          permission: id,
          patterns: [params.subagent_type],
          always: ["*"],
          metadata: {
            description: params.description,
            subagent_type: params.subagent_type,
          },
        })
      }

      const next = yield* agent.get(params.subagent_type)
      if (!next) {
        return yield* Effect.fail(new Error(`Unknown agent type: ${params.subagent_type} is not a valid agent type`))
      }

      const scheduled = yield* scheduler.prepare({
        parentSessionID: ctx.sessionID,
        params,
        messages: ctx.messages,
      })
      const msg = yield* MessageV2.get({ sessionID: ctx.sessionID, messageID: ctx.messageID }).pipe(
        Effect.provideService(Database.Service, database),
        Effect.orDie,
      )
      if (msg.info.role !== "assistant") return yield* Effect.fail(new Error("Not an assistant message"))
      const variant = msg.info.variant
      const model = next.model ?? {
        modelID: msg.info.modelID,
        providerID: msg.info.providerID,
      }

      if (scheduled.type === "duplicate" && scheduled.record.sessionID) {
        const metadata = {
          parentSessionId: ctx.sessionID,
          sessionId: scheduled.record.sessionID,
          model,
          merged: true,
          ...scheduler.metadata(scheduled.record),
          ...(params.memory_scope ? { memoryScope: params.memory_scope } : {}),
          ...(runInBackground ? { background: true } : {}),
        }
        yield* ctx.metadata({
          title: params.description,
          metadata,
        })
        if (scheduled.record.status === "completed" && scheduled.record.output) {
          return {
            title: params.description,
            metadata,
            output: renderOutput({
              sessionID: scheduled.record.sessionID,
              state: "completed",
              summary: "Merged with completed task",
              text: scheduled.record.output,
            }),
          }
        }

        const waited = yield* background.wait({
          id: scheduled.record.sessionID,
          timeout: runInBackground ? 0 : params.timeout_ms,
        })
        if (waited.info?.status === "completed") {
          return {
            title: params.description,
            metadata,
            output: renderOutput({
              sessionID: scheduled.record.sessionID,
              state: "completed",
              summary: "Merged with completed task",
              text: waited.info.output ?? "",
            }),
          }
        }
        if (waited.info?.status === "error") {
          yield* scheduler.fail({ recordID: scheduled.record.id, status: "error", error: waited.info.error })
          return yield* Effect.fail(new Error(waited.info.error ?? "Merged task failed"))
        }
        if (waited.info?.status === "cancelled") {
          yield* scheduler.fail({ recordID: scheduled.record.id, status: "cancelled" })
          return yield* Effect.fail(new Error("Merged task cancelled"))
        }
        return {
          title: params.description,
          metadata: {
            ...metadata,
            background: true,
            jobId: scheduled.record.sessionID,
          },
          output: renderOutput({
            sessionID: scheduled.record.sessionID,
            state: "running",
            summary: waited.timedOut ? "Merged task still running" : "Merged with running task",
            text: MERGED_RUNNING,
          }),
        }
      }

      const session = params.task_id
        ? yield* sessions.get(SessionID.make(params.task_id)).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
        : undefined
      const parent = yield* sessions.get(ctx.sessionID)
      const record = scheduled.record
      const childPermission = deriveSubagentSessionPermission({
        parentSessionPermission: parent.permission ?? [],
        subagent: next,
      })
      const childToolDenies = (target: Agent.Info) => [
        ...(target.permission.some((rule) => rule.permission === "todowrite")
          ? []
          : [{ permission: "todowrite" as const, pattern: "*" as const, action: "deny" as const }]),
        ...(target.permission.some((rule) => rule.permission === id)
          ? []
          : [{ permission: id, pattern: "*" as const, action: "deny" as const }]),
        ...(cfg.experimental?.primary_tools?.map((permission) => ({
          permission,
          pattern: "*" as const,
          action: "deny" as const,
        })) ?? []),
      ]
      const nextSession =
        session ??
        (yield* sessions.create({
          parentID: ctx.sessionID,
          title: params.description + ` (@${next.name} subagent)`,
          agent: next.name,
          metadata: scheduler.metadata(record),
          permission: [
            ...scheduler.ownershipPermissionRules(record),
            ...scheduler.readonlyPermissionRules(record),
            ...childPermission,
            ...childToolDenies(next).filter(
              (deny) =>
                !childPermission.some(
                  (rule) =>
                    rule.permission === deny.permission && rule.pattern === deny.pattern && rule.action === deny.action,
                ),
            ),
          ],
        }))
      const scheduledRecord = yield* scheduler.assignSession({ recordID: record.id, sessionID: nextSession.id })

      const metadata = {
        parentSessionId: ctx.sessionID,
        sessionId: nextSession.id,
        model,
        ...scheduler.metadata(scheduledRecord),
        ...(params.memory_scope ? { memoryScope: params.memory_scope } : {}),
        ...(runInBackground ? { background: true } : {}),
      }

      yield* ctx.metadata({
        title: params.description,
        metadata,
      })

      const ops = ctx.extra?.promptOps as TaskPromptOps
      if (!ops) return yield* Effect.fail(new Error("TaskTool requires promptOps in ctx.extra"))
      const memoryText = yield* SessionMemory.renderForScope(database.db, {
        sessionID: ctx.sessionID,
        projectID: parent.projectID,
        scope: params.memory_scope ?? "none",
      })
      const resolveTaskParts = Effect.fn("TaskTool.resolveTaskParts")(function* (prompt: string) {
        const parts = yield* ops.resolvePromptParts(prompt)
        if (!memoryText) return parts
        return [{ type: "text" as const, synthetic: true, text: memoryText }, ...parts]
      })

      const waitForDependencies = Effect.fn("TaskTool.waitForDependencies")(function* () {
        for (const dependency of scheduledRecord.dependsOn) {
          const waited = yield* background.wait({ id: dependency, timeout: scheduledRecord.timeoutMS })
          if (waited.timedOut) return yield* Effect.fail(new Error(`Timed out waiting for dependency ${dependency}`))
          if (waited.info?.status !== "completed") {
            return yield* Effect.fail(
              new Error(`Task dependency ${dependency} ended with status ${waited.info?.status}`),
            )
          }
        }
      })

      const launchReview = Effect.fn("TaskTool.launchReview")(function* (prompt: string) {
        const reviewer =
          (yield* agent.get("reviewer").pipe(Effect.catchCause(() => Effect.succeed(undefined)))) ??
          (yield* agent.get("review").pipe(Effect.catchCause(() => Effect.succeed(undefined)))) ??
          (yield* agent.get("general").pipe(Effect.catchCause(() => Effect.succeed(undefined))))
        if (!reviewer) return undefined

        const reviewScheduled = yield* scheduler.prepare({
          parentSessionID: ctx.sessionID,
          params: {
            description: "Review conflicts",
            prompt,
            subagent_type: reviewer.name,
            task_kind: "review",
            expected_output: "Resolve conflicting subagent findings with evidence and confidence",
            merge_key: `review:${scheduledRecord.id}`,
          },
          messages: ctx.messages,
        })
        if (reviewScheduled.type === "duplicate" && reviewScheduled.record.sessionID)
          return reviewScheduled.record.sessionID

        const reviewPermission = deriveSubagentSessionPermission({
          parentSessionPermission: parent.permission ?? [],
          subagent: reviewer,
        })
        const reviewSession = yield* sessions.create({
          parentID: ctx.sessionID,
          title: `Review conflicts (@${reviewer.name} subagent)`,
          agent: reviewer.name,
          metadata: scheduler.metadata(reviewScheduled.record),
          permission: [
            ...scheduler.readonlyPermissionRules(reviewScheduled.record),
            ...reviewPermission,
            ...childToolDenies(reviewer).filter(
              (deny) =>
                !reviewPermission.some(
                  (rule) =>
                    rule.permission === deny.permission && rule.pattern === deny.pattern && rule.action === deny.action,
                ),
            ),
          ],
        })
        const reviewRecord = yield* scheduler.assignSession({
          recordID: reviewScheduled.record.id,
          sessionID: reviewSession.id,
        })
        const reviewModel = reviewer.model ?? model
        yield* background.start({
          id: reviewSession.id,
          type: id,
          title: "Review conflicts",
          metadata: {
            parentSessionId: ctx.sessionID,
            sessionId: reviewSession.id,
            model: reviewModel,
            background: true,
            autoReviewFor: nextSession.id,
            ...scheduler.metadata(reviewRecord),
          },
          run: scheduler.run(
            reviewRecord.id,
            Effect.gen(function* () {
              const parts = yield* resolveTaskParts(reviewScheduled.prompt)
              const result = yield* ops.prompt({
                messageID: MessageID.ascending(),
                sessionID: reviewSession.id,
                model: {
                  modelID: reviewModel.modelID,
                  providerID: reviewModel.providerID,
                },
                variant: reviewer.model ? undefined : variant,
                agent: reviewer.name,
                parts,
              })
              const text = result.parts.findLast((item) => item.type === "text")?.text ?? ""
              return (yield* scheduler.complete({ recordID: reviewRecord.id, output: text })).output
            }),
          ),
        })
        return reviewSession.id
      })

      const runTask = Effect.fn("TaskTool.runTask")(function* () {
        const output = yield* scheduler.run(
          scheduledRecord.id,
          Effect.gen(function* () {
            yield* waitForDependencies()
            const parts = yield* resolveTaskParts(scheduled.prompt)
            const result = yield* ops.prompt({
              messageID: MessageID.ascending(),
              sessionID: nextSession.id,
              model: {
                modelID: model.modelID,
                providerID: model.providerID,
              },
              variant: next.model ? undefined : variant,
              agent: next.name,
              parts,
            })
            return result.parts.findLast((item) => item.type === "text")?.text ?? ""
          }),
        )
        const completed = yield* scheduler.complete({
          recordID: scheduledRecord.id,
          output,
        })
        if (!completed.reviewPrompt || scheduledRecord.kind === "review") return completed.output
        const reviewSessionID = yield* launchReview(completed.reviewPrompt).pipe(
          Effect.catchCause((cause) =>
            Effect.logError("auto review subtask failed", { cause }).pipe(Effect.as(undefined)),
          ),
        )
        return [
          completed.output,
          reviewSessionID
            ? `<review_task id="${reviewSessionID}" state="running">Conflict review subagent started.</review_task>`
            : undefined,
        ]
          .filter((line): line is string => typeof line === "string")
          .join("\n\n")
      })

      const inject = Effect.fn("TaskTool.injectBackgroundResult")(function* (
        state: "completed" | "error",
        text: string,
      ) {
        const currentParent = yield* sessions.get(ctx.sessionID)
        yield* ops
          .prompt({
            sessionID: ctx.sessionID,
            agent: currentParent.agent ?? ctx.agent,
            variant,
            parts: [
              {
                type: "text",
                synthetic: true,
                text: renderOutput({
                  sessionID: nextSession.id,
                  state,
                  summary:
                    state === "completed"
                      ? `Background task completed: ${params.description}`
                      : `Background task failed: ${params.description}`,
                  text,
                }),
              },
            ],
          })
          .pipe(Effect.ignore, Effect.forkIn(scope, { startImmediately: true }))
      })

      const notify = Effect.fn("TaskTool.notifyBackgroundResult")(function* (jobID: string) {
        yield* background.wait({ id: jobID }).pipe(
          Effect.flatMap((result) => {
            if (result.info?.status === "completed") return inject("completed", result.info.output ?? "")
            if (result.info?.status === "error")
              return scheduler
                .fail({ recordID: scheduledRecord.id, status: "error", error: result.info.error })
                .pipe(Effect.andThen(inject("error", result.info.error ?? "")))
            if (result.info?.status === "cancelled")
              return scheduler
                .fail({ recordID: scheduledRecord.id, status: "cancelled" })
                .pipe(Effect.andThen(inject("error", "Task cancelled")))
            return Effect.void
          }),
          Effect.forkIn(scope, { startImmediately: true }),
        )
      })

      if (yield* background.extend({ id: nextSession.id, run: runTask() })) {
        return {
          title: params.description,
          metadata: {
            ...metadata,
            background: true,
            jobId: nextSession.id,
          },
          output: renderOutput({
            sessionID: nextSession.id,
            state: "running",
            summary: "Background task updated",
            text: BACKGROUND_UPDATED,
          }),
        }
      }

      const info = yield* background.start({
        id: nextSession.id,
        type: id,
        title: params.description,
        metadata,
        onPromote: Effect.all([
          ctx.metadata({
            title: params.description,
            metadata: { ...metadata, background: true, jobId: nextSession.id },
          }),
          notify(nextSession.id),
        ]),
        run: runTask().pipe(Effect.onInterrupt(() => ops.cancel(nextSession.id))),
      })

      function backgroundResult() {
        return {
          title: params.description,
          metadata: {
            ...metadata,
            background: true,
            jobId: info.id,
          },
          output: renderOutput({
            sessionID: nextSession.id,
            state: "running",
            summary: "Background task started",
            text: BACKGROUND_STARTED,
          }),
        }
      }

      if (runInBackground) {
        yield* notify(info.id)
        return backgroundResult()
      }

      const runCancel = yield* EffectBridge.make()
      const cancel = ops.cancel(nextSession.id)

      function onAbort() {
        runCancel.fork(cancel)
      }

      return yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          ctx.abort.addEventListener("abort", onAbort)
        }),
        () =>
          Effect.gen(function* () {
            const result = yield* Effect.raceFirst(
              background.wait({ id: nextSession.id }).pipe(Effect.map((waited) => waited.info)),
              background.waitForPromotion(nextSession.id),
            )
            if (result?.metadata?.background === true) return backgroundResult()
            if (result?.status === "error") {
              yield* scheduler.fail({ recordID: scheduledRecord.id, status: "error", error: result.error })
              return yield* Effect.fail(new Error(result.error ?? "Task failed"))
            }
            if (result?.status === "cancelled") {
              yield* scheduler.fail({ recordID: scheduledRecord.id, status: "cancelled" })
              return yield* Effect.fail(new Error("Task cancelled"))
            }
            return {
              title: params.description,
              metadata,
              output: renderOutput({ sessionID: nextSession.id, state: "completed", text: result?.output ?? "" }),
            }
          }),
        (_, exit) =>
          Effect.gen(function* () {
            if (Exit.hasInterrupts(exit)) {
              yield* scheduler.fail({ recordID: scheduledRecord.id, status: "cancelled" })
              yield* Effect.all([cancel, background.cancel(nextSession.id)], { discard: true })
            }
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                ctx.abort.removeEventListener("abort", onAbort)
              }),
            ),
          ),
      )
    })

    return {
      description: flags.experimentalBackgroundSubagents
        ? [DESCRIPTION, BACKGROUND_DESCRIPTION].join("\n\n")
        : DESCRIPTION,
      parameters: Parameters,
      jsonSchema: flags.experimentalBackgroundSubagents ? undefined : ToolJsonSchema.fromSchema(BaseParameters),
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
