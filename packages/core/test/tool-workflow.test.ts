import { describe, expect } from "bun:test"
import { Effect, Layer, Schema } from "effect"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { Global } from "@opencode-ai/core/global"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { WorkflowTool } from "@opencode-ai/core/tool/workflow"
import { ToolRegistry } from "@opencode-ai/core/tool/registry"
import { WorkflowRuntime } from "@opencode-ai/core/workflow"
import { testEffect } from "./lib/effect"
import { executeTool, settleTool, toolDefinitions, toolIdentity } from "./lib/tool"

const sessionID = SessionV2.ID.make("ses_workflow_tool_test")
const directory = AbsolutePath.make("/project")
const data = path.join(os.tmpdir(), `opencode-workflow-tool-test-${process.pid}`)
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
const global = Global.layerWith({ data })
const workflows = WorkflowRuntime.layer.pipe(Layer.provide(database), Layer.provide(global))
const registry = ToolRegistry.defaultLayer.pipe(Layer.provide(permission))
const tool = WorkflowTool.layer.pipe(Layer.provide(registry), Layer.provide(permission), Layer.provide(workflows))
const it = testEffect(Layer.mergeAll(database, global, workflows, permission, registry, tool))

const setup = Effect.gen(function* () {
  assertions.length = 0
  deny = false
  yield* Effect.promise(() => fs.rm(data, { recursive: true, force: true }))
  const { db } = yield* Database.Service
  yield* db.delete(SessionTable).run().pipe(Effect.orDie)
  yield* db.delete(ProjectTable).run().pipe(Effect.orDie)
  yield* db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: directory, sandboxes: [] })
    .run()
    .pipe(Effect.orDie)
  yield* db
    .insert(SessionTable)
    .values({
      id: sessionID,
      project_id: Project.ID.global,
      slug: "workflow-tool",
      directory,
      title: "workflow tool",
      version: "test",
    })
    .run()
    .pipe(Effect.orDie)
})

const call = (operation: unknown, id = "call-workflow") => ({
  sessionID,
  ...toolIdentity,
  call: { type: "tool-call" as const, id, name: WorkflowTool.name, input: { operation } },
})

describe("WorkflowTool", () => {
  it.effect("starts, waits, and lists durable workflow runs", () =>
    Effect.gen(function* () {
      yield* setup
      const registry = yield* ToolRegistry.Service

      expect((yield* toolDefinitions(registry)).map((tool) => tool.name)).toEqual([WorkflowTool.name])
      const started = yield* settleTool(
        registry,
        call({
          action: "run",
          name: "scripted",
          args: { ok: true },
          script: "await phase('compute'); await log('running'); return { ok: args.ok }",
        }),
      )
      const startOutput = Schema.decodeUnknownSync(WorkflowTool.Output)(started.output?.structured)
      const runID = startOutput.run?.runID
      if (!runID) return yield* Effect.die("missing workflow run id")

      const waited = yield* settleTool(registry, call({ action: "wait", runID }, "call-workflow-wait"))
      expect(Schema.decodeUnknownSync(WorkflowTool.Output)(waited.output?.structured).outcome).toEqual({
        status: "completed",
        result: { ok: true },
      })

      const listed = yield* settleTool(registry, call({ action: "list" }, "call-workflow-list"))
      expect(Schema.decodeUnknownSync(WorkflowTool.Output)(listed.output?.structured).runs?.[0]).toMatchObject({
        runID,
        status: "completed",
        name: "scripted",
      })
      expect(assertions).toMatchObject([
        { sessionID, action: "workflow", resources: ["*"], save: ["*"] },
        { sessionID, action: "workflow", resources: ["*"], save: ["*"] },
        { sessionID, action: "workflow", resources: ["*"], save: ["*"] },
      ])
    }),
  )

  it.effect("does not start workflows when permission is denied", () =>
    Effect.gen(function* () {
      yield* setup
      const registry = yield* ToolRegistry.Service
      const workflows = yield* WorkflowRuntime.Service
      deny = true

      expect(yield* executeTool(registry, call({ action: "run", script: "return true" }))).toEqual({
        type: "error",
        value: "Unable to use workflow runtime",
      })
      expect(yield* workflows.list({ sessionID })).toEqual([])
    }),
  )

  it.effect("fails agent workflows when no workflow agent hook is installed", () =>
    Effect.gen(function* () {
      yield* setup
      const registry = yield* ToolRegistry.Service

      const started = yield* settleTool(registry, call({ action: "run", script: "return await agent('missing hook')" }))
      const runID = Schema.decodeUnknownSync(WorkflowTool.Output)(started.output?.structured).run?.runID
      if (!runID) return yield* Effect.die("missing workflow run id")

      const waited = yield* settleTool(registry, call({ action: "wait", runID }, "call-workflow-missing-agent-wait"))
      expect(Schema.decodeUnknownSync(WorkflowTool.Output)(waited.output?.structured).outcome).toMatchObject({
        status: "failed",
      })

      const status = yield* settleTool(registry, call({ action: "status", runID }, "call-workflow-missing-agent-status"))
      expect(Schema.decodeUnknownSync(WorkflowTool.Output)(status.output?.structured).run).toMatchObject({
        runID,
        status: "failed",
        succeeded: 0,
        failed: 1,
      })
    }),
  )
})
