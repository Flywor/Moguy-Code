import { describe, expect } from "bun:test"
import { Effect, Layer, Schema } from "effect"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { Project } from "@opencode-ai/core/project"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionMemorySearch } from "@opencode-ai/core/session/memory-search"
import { MemoryTool } from "@opencode-ai/core/tool/memory"
import { ToolRegistry } from "@opencode-ai/core/tool/registry"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"
import { settleTool, toolDefinitions, toolIdentity } from "./lib/tool"

const sessionID = SessionV2.ID.make("ses_memory_tool_test")
const data = path.join(os.tmpdir(), `opencode-memory-tool-test-${process.pid}`)
const assertions: PermissionV2.AssertInput[] = []

const permission = Layer.succeed(
  PermissionV2.Service,
  PermissionV2.Service.of({
    assert: (input) => Effect.sync(() => assertions.push(input)),
    ask: () => Effect.die("unused"),
    reply: () => Effect.die("unused"),
    get: () => Effect.die("unused"),
    forSession: () => Effect.die("unused"),
    list: () => Effect.die("unused"),
  }),
)
const database = Database.layerFromPath(":memory:")
const global = Global.layerWith({ data })
const directory = AbsolutePath.make("/project")
const locationLayer = Layer.succeed(
  Location.Service,
  Location.Service.of(location({ directory }, { projectDirectory: directory })),
)
const memory = SessionMemorySearch.layer.pipe(Layer.provide(database), Layer.provide(global))
const registry = ToolRegistry.defaultLayer.pipe(Layer.provide(permission))
const tool = MemoryTool.layer.pipe(
  Layer.provide(registry),
  Layer.provide(permission),
  Layer.provide(memory),
  Layer.provide(locationLayer),
)
const it = testEffect(Layer.mergeAll(database, global, locationLayer, memory, permission, registry, tool))

const setup = Effect.promise(async () => {
  assertions.length = 0
  await fs.rm(data, { recursive: true, force: true })
  const file = path.join(data, "memory", "projects", "project-a", "checkpoint.md")
  await fs.mkdir(path.dirname(file), { recursive: true })
  await Bun.write(
    file,
    ["# Checkpoint", "", "The stop gate task registry keeps open and in_progress work from being forgotten."].join("\n"),
  )
})

const call = (input: unknown) => ({
  sessionID,
  ...toolIdentity,
  call: { type: "tool-call" as const, id: "call-memory", name: MemoryTool.name, input },
})

describe("MemoryTool", () => {
  it.effect("reconciles markdown memory and returns FTS-ranked snippets", () =>
    Effect.gen(function* () {
      yield* setup
      const registry = yield* ToolRegistry.Service

      expect((yield* toolDefinitions(registry)).map((tool) => tool.name)).toEqual([MemoryTool.name])
      const result = yield* settleTool(registry, call({ query: "stop gate registry" }))
      const output = Schema.decodeUnknownSync(MemoryTool.Output)(result.output?.structured)

      expect(output.results ?? []).toHaveLength(1)
      expect(output.results?.[0]).toMatchObject({
        scope: "projects",
        scopeID: "project-a",
        type: "checkpoint",
      })
      expect(String(result.result.value)).toContain("stop")
      expect(assertions).toMatchObject([{ sessionID, action: "memory", resources: ["*"], save: ["*"] }])
    }),
  )

  it.effect("writes markdown memory under the current project and indexes it immediately", () =>
    Effect.gen(function* () {
      yield* setup
      const registry = yield* ToolRegistry.Service

      const written = yield* settleTool(
        registry,
        call({ operation: "write", key: "MEMORY", body: "# Project Memory\n\nRelease workflow uses staged checks." }),
      )
      expect(Schema.decodeUnknownSync(MemoryTool.Output)(written.output?.structured)).toMatchObject({
        path: path.join(data, "memory", "projects", Project.ID.global, "MEMORY.md"),
        scope: "projects",
        scopeID: Project.ID.global,
        type: "memory",
        existed: false,
      })

      const searched = yield* settleTool(registry, call({ query: "release workflow", scopeID: Project.ID.global }))
      const output = Schema.decodeUnknownSync(MemoryTool.Output)(searched.output?.structured)
      expect(output.results?.[0]).toMatchObject({
        scope: "projects",
        scopeID: Project.ID.global,
        type: "memory",
      })
    }),
  )
})
