export * as TaskTool from "./task"

import { ToolFailure } from "@opencode-ai/llm"
import { Effect, Layer, Schema } from "effect"
import { PermissionV2 } from "../permission"
import { SessionSchema } from "../session/schema"
import { SessionTask } from "../session/task"
import { Tool } from "./tool"
import { Tools } from "./tools"

export const name = "task"

const SessionID = SessionSchema.ID.pipe(Schema.optional).annotate({
  description: "Session id to act on. Defaults to the current session.",
})

const CreateOperation = Schema.Struct({
  action: Schema.Literal("create"),
  summary: Schema.String.annotate({ description: "Task summary for a single bounded work item." }),
  parentTaskID: Schema.String.pipe(Schema.optional).annotate({ description: "Optional parent task id." }),
  sessionID: SessionID,
})

const ListOperation = Schema.Struct({
  action: Schema.Literal("list"),
  status: SessionTask.Status.pipe(Schema.optional).annotate({ description: "Optional status filter." }),
  includeTerminal: Schema.Boolean.pipe(Schema.optional).annotate({
    description: "Include done and abandoned tasks. Default false.",
  }),
  includeArchived: Schema.Boolean.pipe(Schema.optional).annotate({
    description: "Include tasks past their cleanup window. Default false.",
  }),
  sessionID: SessionID,
})

const IDOperationBase = {
  id: Schema.String.annotate({ description: "Task id, for example T1 or T1.1." }),
  sessionID: SessionID,
}

const GetOperation = Schema.Struct({ action: Schema.Literal("get"), ...IDOperationBase })
const StartOperation = Schema.Struct({
  action: Schema.Literal("start"),
  ...IDOperationBase,
  eventSummary: Schema.String.pipe(Schema.optional).annotate({ description: "Short note on starting." }),
})
const BlockOperation = Schema.Struct({
  action: Schema.Literal("block"),
  ...IDOperationBase,
  eventSummary: Schema.String.pipe(Schema.optional).annotate({ description: "Short reason for blocking." }),
})
const UnblockOperation = Schema.Struct({
  action: Schema.Literal("unblock"),
  ...IDOperationBase,
  eventSummary: Schema.String.pipe(Schema.optional).annotate({ description: "Short reason for unblocking." }),
})
const DoneOperation = Schema.Struct({
  action: Schema.Literal("done"),
  ...IDOperationBase,
  eventSummary: Schema.String.pipe(Schema.optional).annotate({ description: "Short summary of completed work." }),
})
const AbandonOperation = Schema.Struct({
  action: Schema.Literal("abandon"),
  ...IDOperationBase,
  eventSummary: Schema.String.pipe(Schema.optional).annotate({ description: "Short reason for abandoning." }),
})
const RenameOperation = Schema.Struct({
  action: Schema.Literal("rename"),
  ...IDOperationBase,
  summary: Schema.String.annotate({ description: "New task summary." }),
})
const EventsOperation = Schema.Struct({ action: Schema.Literal("events"), ...IDOperationBase })

const Operation = Schema.Union([
  CreateOperation,
  ListOperation,
  GetOperation,
  StartOperation,
  BlockOperation,
  UnblockOperation,
  DoneOperation,
  AbandonOperation,
  RenameOperation,
  EventsOperation,
]).annotate({ description: "Task registry operation." })

export const Input = Schema.Struct({
  operation: Operation,
})

export const Output = Schema.Struct({
  message: Schema.String,
  task: SessionTask.Info.pipe(Schema.optional),
  tasks: Schema.Array(SessionTask.Info).pipe(Schema.optional),
  events: Schema.Array(SessionTask.EventInfo).pipe(Schema.optional),
})
export type Output = typeof Output.Type

export const toModelOutput = (output: typeof Output.Encoded) =>
  [
    output.message,
    output.task ? `\nTask:\n${JSON.stringify(output.task, null, 2)}` : "",
    output.tasks ? `\nTasks:\n${JSON.stringify(output.tasks, null, 2)}` : "",
    output.events ? `\nEvents:\n${JSON.stringify(output.events, null, 2)}` : "",
  ].join("")

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const tools = yield* Tools.Service
    const tasks = yield* SessionTask.Service
    const permission = yield* PermissionV2.Service

    yield* tools
      .register({
        [name]: Tool.make({
          description: [
            "Persistent work-item registry with explicit task states: open, in_progress, blocked, done, abandoned.",
            "Use it for multi-step work that must not be forgotten before stopping.",
            "Mark a task start before working on it, done after finishing it, block when genuinely waiting, and abandon only when no longer needed.",
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
              const sessionID = operation.sessionID ?? context.sessionID
              if (operation.action === "create") {
                const task = yield* tasks.create({
                  sessionID,
                  summary: operation.summary,
                  parentTaskID: operation.parentTaskID,
                  owner: context.agent,
                })
                return { message: `Created task ${task.id}.`, task }
              }
              if (operation.action === "list") {
                const list = yield* tasks.list({
                  sessionID,
                  status: operation.status,
                  includeTerminal: operation.includeTerminal,
                  includeArchived: operation.includeArchived,
                })
                return { message: `Found ${list.length} task${list.length === 1 ? "" : "s"}.`, tasks: list }
              }
              if (operation.action === "get") {
                const task = yield* tasks.get({ sessionID, id: operation.id })
                return task ? { message: `Loaded task ${task.id}.`, task } : { message: `Task ${operation.id} not found.` }
              }
              if (operation.action === "events") {
                const events = yield* tasks.events({ sessionID, taskID: operation.id })
                return { message: `Found ${events.length} event${events.length === 1 ? "" : "s"}.`, events }
              }
              if (operation.action === "rename") {
                const task = yield* tasks.rename({ sessionID, id: operation.id, summary: operation.summary })
                return { message: `Renamed task ${task.id}.`, task }
              }
              const task = yield* tasks[operation.action]({
                sessionID,
                id: operation.id,
                eventSummary: operation.eventSummary,
              })
              return { message: `Marked task ${task.id} ${task.status}.`, task }
            }).pipe(Effect.mapError(() => new ToolFailure({ message: "Unable to update task registry" }))),
        }),
      })
      .pipe(Effect.orDie)
  }),
)
