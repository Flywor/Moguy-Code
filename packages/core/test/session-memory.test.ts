import { describe, expect } from "bun:test"
import { DateTime, Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { Location } from "@opencode-ai/core/location"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionMemory } from "@opencode-ai/core/session/memory"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SystemContext } from "@opencode-ai/core/system-context"
import { SystemContextRegistry } from "@opencode-ai/core/system-context/registry"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"

const database = Database.layerFromPath(":memory:")
const locationLayer = Layer.succeed(
  Location.Service,
  Location.Service.of(location(Location.Ref.make({ directory: AbsolutePath.make("/project") }))),
)
const it = testEffect(
  SessionMemory.locationLayer.pipe(
    Layer.provideMerge(SystemContextRegistry.layer),
    Layer.provideMerge(database),
    Layer.provideMerge(locationLayer),
  ),
)
const firstSession = SessionSchema.ID.make("ses_memory_first")
const secondSession = SessionSchema.ID.make("ses_memory_second")

const seedSession = Effect.fn("SessionMemoryTest.seedSession")(function* (input: {
  readonly sessionID: SessionSchema.ID
  readonly title: string
}) {
  const { db } = yield* Database.Service
  yield* db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
  yield* db
    .insert(SessionTable)
    .values({
      id: input.sessionID,
      project_id: Project.ID.global,
      slug: input.title,
      directory: "/project",
      title: input.title,
      version: "test",
    })
    .run()
    .pipe(Effect.orDie)
})

describe("SessionMemory", () => {
  it.effect("stores latest compaction summaries as session and project memory", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* seedSession({ sessionID: firstSession, title: "First" })
      yield* seedSession({ sessionID: secondSession, title: "Second" })

      yield* SessionMemory.rememberCompaction(db, {
        sessionID: firstSession,
        messageID: SessionMessage.ID.make("msg_first"),
        timestamp: DateTime.makeUnsafe(1),
        summary: "## Goal\n- Keep the API stable\n\n## Key Decisions\n- Preserve message IDs",
        recent: "recent first",
      })
      yield* SessionMemory.rememberCompaction(db, {
        sessionID: secondSession,
        messageID: SessionMessage.ID.make("msg_second"),
        timestamp: DateTime.makeUnsafe(2),
        summary: "## Goal\n- Add project memory\n\n## Relevant Files\n- src/session/memory.ts",
        recent: "recent second",
      })
      yield* SessionMemory.rememberCompaction(db, {
        sessionID: firstSession,
        messageID: SessionMessage.ID.make("msg_first_updated"),
        timestamp: DateTime.makeUnsafe(3),
        summary: "## Goal\n- Keep the API stable\n\n## Key Decisions\n- Use one row per session",
        recent: "recent first updated",
      })

      expect(yield* SessionMemory.loadSession(db, firstSession)).toMatchObject({
        sessionID: firstSession,
        title: "First",
        summary: expect.stringContaining("Use one row per session"),
        recent: "recent first updated",
      })
      expect((yield* SessionMemory.loadProject(db, Project.ID.global)).map((entry) => entry.sessionID)).toEqual([
        firstSession,
        secondSession,
      ])
      expect(
        yield* SessionMemory.renderForScope(db, {
          sessionID: firstSession,
          projectID: Project.ID.global,
          scope: "session-project",
        }),
      ).toContain("<session_memory>")
      expect(
        yield* SessionMemory.renderForScope(db, {
          sessionID: firstSession,
          projectID: Project.ID.global,
          scope: "session-project",
        }),
      ).toContain("<project_memory>")
    }),
  )

  it.effect("registers project memory as system context", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* seedSession({ sessionID: firstSession, title: "First" })
      yield* SessionMemory.rememberCompaction(db, {
        sessionID: firstSession,
        messageID: SessionMessage.ID.make("msg_context"),
        timestamp: DateTime.makeUnsafe(1),
        summary: "## Critical Context\n- The migration is already generated",
        recent: "recent",
      })

      const registry = yield* SystemContextRegistry.Service
      const initialized = yield* SystemContext.initialize(yield* registry.load())

      expect(initialized.baseline).toContain("<project_memory>")
      expect(initialized.baseline).toContain("The migration is already generated")
    }),
  )
})
