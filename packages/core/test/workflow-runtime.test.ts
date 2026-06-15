import { describe, expect } from "bun:test"
import { Deferred, Effect, Layer } from "effect"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { Global } from "@opencode-ai/core/global"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { WorkflowAgent, WorkflowRuntime, WorkflowWorkspace } from "@opencode-ai/core/workflow"
import { testEffect } from "./lib/effect"

const sessionID = SessionV2.ID.make("ses_workflow_runtime_test")
const directory = AbsolutePath.make("/project")
const data = path.join(os.tmpdir(), `opencode-workflow-runtime-test-${process.pid}`)
const database = Database.layerFromPath(":memory:")
const global = Global.layerWith({ data })
const workflow = WorkflowRuntime.layer.pipe(Layer.provide(database), Layer.provide(global))
const it = testEffect(Layer.mergeAll(database, global, workflow))
const agentCalls: WorkflowAgent.Input[] = []
const workflowAgent = Layer.succeed(
  WorkflowAgent.Service,
  WorkflowAgent.Service.of({
    run: (input) =>
      Effect.sync(() => {
        agentCalls.push(input)
        return input.prompt.toUpperCase()
      }),
  }),
)
const workflowWithAgent = WorkflowRuntime.layer.pipe(
  Layer.provide(database),
  Layer.provide(global),
  Layer.provide(workflowAgent),
)
const itWithAgent = testEffect(Layer.mergeAll(database, global, workflowAgent, workflowWithAgent))
const bridgeAgentCalls: WorkflowAgent.Input[] = []
const workflowAgentBridge = Layer.effectDiscard(
  WorkflowAgent.install(
    WorkflowAgent.Service.of({
      run: (input) =>
        Effect.sync(() => {
          bridgeAgentCalls.push(input)
          return `bridge:${input.prompt}`
        }),
    }),
  ),
)
const workflowAgentFromRef = WorkflowAgent.layerFromRef
const workflowWithAgentBridge = WorkflowRuntime.layer.pipe(
  Layer.provide(database),
  Layer.provide(global),
  Layer.provide(workflowAgentFromRef),
)
const itWithAgentBridge = testEffect(
  Layer.mergeAll(database, global, workflowAgentBridge, workflowAgentFromRef, workflowWithAgentBridge),
)
const workspaceCreates: WorkflowWorkspace.CreateInput[] = []
const workspaceReleases: Array<{ readonly directory: string; readonly force: boolean }> = []
const workflowWorkspace = Layer.succeed(
  WorkflowWorkspace.Service,
  WorkflowWorkspace.Service.of({
    create: (input) =>
      Effect.sync(() => {
        workspaceCreates.push(input)
        return {
          directory: AbsolutePath.make(path.join(data, "managed-worktrees", input.name ?? input.runID)),
          managed: true,
          removeOnFinish: input.removeOnFinish ?? false,
          removeOnCancel: input.removeOnCancel ?? true,
          forceRemove: input.forceRemove ?? true,
        }
      }),
    release: (input) => Effect.sync(() => workspaceReleases.push(input)),
  }),
)
const workflowWithWorkspace = WorkflowRuntime.layer.pipe(
  Layer.provide(database),
  Layer.provide(global),
  Layer.provide(workflowWorkspace),
)
const itWithWorkspace = testEffect(Layer.mergeAll(database, global, workflowWorkspace, workflowWithWorkspace))

const setup = Effect.gen(function* () {
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
      slug: "workflow",
      directory,
      title: "workflow",
      version: "test",
    })
    .run()
    .pipe(Effect.orDie)
})

describe("WorkflowRuntime", () => {
  it.live("runs scripts in a restricted context", () =>
    Effect.gen(function* () {
      yield* setup
      const workflows = yield* WorkflowRuntime.Service

      const started = yield* workflows.start({
        sessionID,
        name: "sandbox",
        script: "return typeof process === 'undefined' && typeof require === 'undefined' && args.ok === true",
        args: { ok: true },
      })

      expect(yield* workflows.wait({ runID: started.runID })).toEqual({ status: "completed", result: true })
      expect(yield* workflows.status(started.runID)).toMatchObject({
        status: "completed",
        result: true,
      })
    }),
  )

  it.live("exposes workspace-jailed file primitives to scripts", () =>
    Effect.gen(function* () {
      yield* setup
      const workflows = yield* WorkflowRuntime.Service
      const workspace = path.join(data, "workspace-file-hooks")
      yield* Effect.promise(async () => {
        await fs.rm(workspace, { recursive: true, force: true })
        await fs.mkdir(workspace, { recursive: true })
        await Bun.write(path.join(workspace, "input.txt"), "checkpoint")
        await Bun.write(path.join(data, "outside.txt"), "outside")
      })

      const started = yield* workflows.start({
        sessionID,
        name: "files",
        workspace,
        script: [
          "await writeFile('out/result.txt', await readFile('input.txt'))",
          "const escaped = await readFile('../outside.txt').then(() => false, () => true)",
          "return {",
          "  body: await readFile('out/result.txt'),",
          "  exists: await exists('out/result.txt'),",
          "  files: await glob('**/*.txt'),",
          "  escaped,",
          "}",
        ].join("\n"),
      })

      expect(yield* workflows.wait({ runID: started.runID })).toEqual({
        status: "completed",
        result: {
          body: "checkpoint",
          exists: true,
          files: ["input.txt", "out/result.txt"],
          escaped: true,
        },
      })
    }),
  )

  it.live("bounds concurrent agent calls", () =>
    Effect.gen(function* () {
      yield* setup
      const workflows = yield* WorkflowRuntime.Service
      let active = 0
      let maxActive = 0

      const started = yield* workflows.start({
        sessionID,
        name: "parallel",
        maxConcurrentAgents: 2,
        script: "return await parallel(['a', 'b', 'c', 'd'].map((x) => () => agent(x)))",
        agent: (input) =>
          Effect.gen(function* () {
            active++
            maxActive = Math.max(maxActive, active)
            yield* Effect.sleep("10 millis")
            active--
            return input.prompt.toUpperCase()
          }),
      })

      expect(yield* workflows.wait({ runID: started.runID })).toEqual({
        status: "completed",
        result: ["A", "B", "C", "D"],
      })
      expect(maxActive).toBeLessThanOrEqual(2)
      expect(yield* workflows.status(started.runID)).toMatchObject({
        status: "completed",
        succeeded: 4,
        failed: 0,
      })
    }),
  )

  itWithAgent.live("uses the workflow agent service when no start hook is provided", () =>
    Effect.gen(function* () {
      yield* setup
      agentCalls.length = 0
      const workflows = yield* WorkflowRuntime.Service

      const started = yield* workflows.start({
        sessionID,
        name: "service-agent",
        script: "return await agent('delegate this')",
      })

      expect(yield* workflows.wait({ runID: started.runID })).toEqual({
        status: "completed",
        result: "DELEGATE THIS",
      })
      expect(agentCalls).toMatchObject([{ runID: started.runID, sessionID, prompt: "delegate this" }])
    }),
  )

  itWithAgentBridge.live("uses the workflow agent bridge from runtime services", () =>
    Effect.gen(function* () {
      yield* setup
      bridgeAgentCalls.length = 0
      const workflows = yield* WorkflowRuntime.Service

      const started = yield* workflows.start({
        sessionID,
        name: "bridge-agent",
        script: "return await agent('delegate bridge')",
      })

      expect(yield* workflows.wait({ runID: started.runID })).toEqual({
        status: "completed",
        result: "bridge:delegate bridge",
      })
      expect(bridgeAgentCalls).toMatchObject([{ runID: started.runID, sessionID, prompt: "delegate bridge" }])
    }),
  )

  itWithWorkspace.live("creates a managed worktree workspace and releases it on finish", () =>
    Effect.gen(function* () {
      yield* setup
      workspaceCreates.length = 0
      workspaceReleases.length = 0
      const workflows = yield* WorkflowRuntime.Service

      const started = yield* workflows.start({
        sessionID,
        name: "managed-finish",
        worktree: { name: "finish-worktree", removeOnFinish: true },
        script: "return workspace",
      })

      const managed = path.join(data, "managed-worktrees", "finish-worktree")
      expect(yield* workflows.wait({ runID: started.runID })).toEqual({
        status: "completed",
        result: managed,
      })
      expect(yield* workflows.status(started.runID)).toMatchObject({
        workspace: managed,
        workspaceManaged: true,
        workspaceRemoveOnFinish: true,
      })
      expect(workspaceCreates).toMatchObject([{ runID: started.runID, name: "finish-worktree", removeOnFinish: true }])
      expect(workspaceReleases).toEqual([{ directory: managed, force: true }])
    }),
  )

  itWithWorkspace.live("releases a managed worktree workspace on cancel", () =>
    Effect.gen(function* () {
      yield* setup
      workspaceCreates.length = 0
      workspaceReleases.length = 0
      const workflows = yield* WorkflowRuntime.Service
      const entered = yield* Deferred.make<void>()

      const started = yield* workflows.start({
        sessionID,
        name: "managed-cancel",
        worktree: { name: "cancel-worktree" },
        script: "return await agent('wait')",
        agent: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      })

      const managed = path.join(data, "managed-worktrees", "cancel-worktree")
      yield* Deferred.await(entered).pipe(Effect.timeout("1 second"))
      expect(yield* workflows.cancel(started.runID)).toMatchObject({
        status: "cancelled",
        workspace: managed,
        workspaceManaged: true,
      })
      expect(workspaceReleases).toEqual([{ directory: managed, force: true }])
    }),
  )

  it.live("replays journaled agent results on resume", () =>
    Effect.gen(function* () {
      yield* setup
      const workflows = yield* WorkflowRuntime.Service
      let calls = 0

      const started = yield* workflows.start({
        sessionID,
        name: "resume",
        script: [
          "await agent('one')",
          "await agent('two')",
          "throw new Error('boom after journal')",
        ].join("\n"),
        agent: (input) =>
          Effect.sync(() => {
            calls++
            return input.prompt
          }),
      })

      expect(yield* workflows.wait({ runID: started.runID })).toMatchObject({ status: "failed" })
      expect(calls).toBe(2)
      expect(yield* workflows.resume({ runID: started.runID, agent: () => Effect.die("should replay") })).toEqual({
        runID: started.runID,
        resumed: true,
      })
      expect(yield* workflows.wait({ runID: started.runID })).toMatchObject({ status: "failed" })
      expect(calls).toBe(2)
    }),
  )

  it.live("cancels running workflow scripts", () =>
    Effect.gen(function* () {
      yield* setup
      const workflows = yield* WorkflowRuntime.Service
      const entered = yield* Deferred.make<void>()

      const started = yield* workflows.start({
        sessionID,
        name: "cancel",
        script: "return await agent('wait')",
        agent: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      })

      yield* Deferred.await(entered).pipe(Effect.timeout("1 second"))
      expect(yield* workflows.cancel(started.runID)).toMatchObject({ status: "cancelled" })
      expect(yield* workflows.wait({ runID: started.runID })).toEqual({ status: "cancelled" })
    }),
  )
})
