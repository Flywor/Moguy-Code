import { describe, expect } from "bun:test"
import { Effect, Layer, Schema } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionTask } from "@opencode-ai/core/session/task"
import { TaskTool } from "@opencode-ai/core/tool/task"
import { ToolRegistry } from "@opencode-ai/core/tool/registry"
import { testEffect } from "./lib/effect"
import { executeTool, settleTool, toolDefinitions, toolIdentity } from "./lib/tool"

const sessionID = SessionV2.ID.make("ses_task_tool_test")
const assertions: PermissionV2.AssertInput[] = []
let deny = false

const permission = Layer.succeed(
  PermissionV2.Service,
  PermissionV2.Service.of({
    assert: (input) =>
      Effect.sync(() => assertions.push(input)).pipe(
        Effect.andThen(deny ? Effect.fail(new PermissionV2.DeniedError({ rules: [] })) : Effect.void),
      ),
    ask: () => Effect.die("unused"),
    reply: () => Effect.die("unused"),
    get: () => Effect.die("unused"),
    forSession: () => Effect.die("unused"),
    list: () => Effect.die("unused"),
  }),
)
const database = Database.layerFromPath(":memory:")
const tasks = SessionTask.layer.pipe(Layer.provide(database))
const registry = ToolRegistry.defaultLayer.pipe(Layer.provide(permission))
const tool = TaskTool.layer.pipe(Layer.provide(registry), Layer.provide(permission), Layer.provide(tasks))
const it = testEffect(Layer.mergeAll(database, tasks, permission, registry, tool))

const setup = Effect.gen(function* () {
  assertions.length = 0
  deny = false
  const { db } = yield* Database.Service
  yield* db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .run()
    .pipe(Effect.orDie)
  yield* db
    .insert(SessionTable)
    .values({
      id: sessionID,
      project_id: Project.ID.global,
      slug: "task",
      directory: "/project",
      title: "task",
      version: "test",
    })
    .run()
    .pipe(Effect.orDie)
})

const call = (operation: unknown, id = "call-task") => ({
  sessionID,
  ...toolIdentity,
  call: { type: "tool-call" as const, id, name: TaskTool.name, input: { operation } },
})

describe("TaskTool", () => {
  it.effect("registers, persists state transitions, and records task events", () =>
    Effect.gen(function* () {
      yield* setup
      const registry = yield* ToolRegistry.Service
      const service = yield* SessionTask.Service

      expect((yield* toolDefinitions(registry)).map((tool) => tool.name)).toEqual([TaskTool.name])
      const created = yield* settleTool(registry, call({ action: "create", summary: "Implement stop gate" }))
      expect(Schema.decodeUnknownSync(TaskTool.Output)(created.output?.structured).task).toMatchObject({
        id: "T1",
        status: "open",
        summary: "Implement stop gate",
        owner: "build",
      })

      yield* executeTool(registry, call({ action: "start", id: "T1" }, "call-start"))
      yield* executeTool(registry, call({ action: "done", id: "T1", eventSummary: "Verified" }, "call-done"))
      yield* executeTool(registry, call({ action: "block", id: "T1", eventSummary: "Too late" }, "call-block-done"))

      expect(yield* service.get({ sessionID, id: "T1" })).toMatchObject({ id: "T1", status: "done" })
      expect((yield* service.events({ sessionID, taskID: "T1" })).map((event) => event.kind)).toEqual([
        "created",
        "started",
        "done",
      ])
      expect(assertions).toMatchObject([
        { sessionID, action: "task", resources: ["*"], save: ["*"] },
        { sessionID, action: "task", resources: ["*"], save: ["*"] },
        { sessionID, action: "task", resources: ["*"], save: ["*"] },
        { sessionID, action: "task", resources: ["*"], save: ["*"] },
      ])
    }),
  )

  it.effect("does not mutate task state when permission is denied", () =>
    Effect.gen(function* () {
      yield* setup
      const registry = yield* ToolRegistry.Service
      const service = yield* SessionTask.Service
      yield* service.create({ sessionID, summary: "Keep open" })
      deny = true

      expect(yield* executeTool(registry, call({ action: "done", id: "T1" }))).toEqual({
        type: "error",
        value: "Unable to update task registry",
      })
      expect(yield* service.get({ sessionID, id: "T1" })).toMatchObject({ id: "T1", status: "open" })
    }),
  )
})
