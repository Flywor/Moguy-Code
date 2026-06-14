export * as SessionMemory from "./memory"

import { and, desc, eq, ne } from "drizzle-orm"
import { DateTime, Effect, Layer, Schema } from "effect"
import { Database } from "../database/database"
import { Location } from "../location"
import { ProjectV2 } from "../project"
import { SystemContext } from "../system-context/index"
import { SystemContextRegistry } from "../system-context/registry"
import { SessionMessage } from "./message"
import { SessionSchema } from "./schema"
import { SessionMemoryTable, SessionTable } from "./sql"

const PROJECT_MEMORY_LIMIT = 8
const ENTRY_CHAR_LIMIT = 6_000
const CONTEXT_CHAR_LIMIT = 32_000
const PROJECT_CONTEXT_KEY = SystemContext.Key.make("memory/project")

type DatabaseService = Database.Interface["db"]

export const Scope = Schema.Literals(["none", "session", "project", "session-project"]).annotate({
  identifier: "SessionMemory.Scope",
})
export type Scope = typeof Scope.Type

const ProjectSnapshotEntry = Schema.Struct({
  sessionID: SessionSchema.ID,
  title: Schema.String,
  summary: Schema.String,
  timeUpdated: Schema.Finite,
})
type ProjectSnapshotEntry = typeof ProjectSnapshotEntry.Type

type MemoryEntry = ProjectSnapshotEntry & {
  readonly recent: string
}

export const rememberCompaction = Effect.fn("SessionMemory.rememberCompaction")(function* (
  db: DatabaseService,
  input: {
    readonly sessionID: SessionSchema.ID
    readonly messageID: SessionMessage.ID
    readonly timestamp: DateTime.Utc
    readonly summary: string
    readonly recent: string
  },
) {
  const session = yield* db
    .select({ projectID: SessionTable.project_id })
    .from(SessionTable)
    .where(eq(SessionTable.id, input.sessionID))
    .get()
    .pipe(Effect.orDie)
  if (!session) return
  const now = DateTime.toEpochMillis(input.timestamp)
  yield* db
    .insert(SessionMemoryTable)
    .values({
      session_id: input.sessionID,
      project_id: session.projectID,
      source_message_id: input.messageID,
      summary: input.summary,
      recent: input.recent,
      time_created: now,
      time_updated: now,
    })
    .onConflictDoUpdate({
      target: SessionMemoryTable.session_id,
      set: {
        project_id: session.projectID,
        source_message_id: input.messageID,
        summary: input.summary,
        recent: input.recent,
        time_updated: now,
      },
    })
    .run()
    .pipe(Effect.orDie)
})

export const loadSession = Effect.fn("SessionMemory.loadSession")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
) {
  const row = yield* db
    .select({
      sessionID: SessionMemoryTable.session_id,
      title: SessionTable.title,
      summary: SessionMemoryTable.summary,
      recent: SessionMemoryTable.recent,
      timeUpdated: SessionMemoryTable.time_updated,
    })
    .from(SessionMemoryTable)
    .innerJoin(SessionTable, eq(SessionTable.id, SessionMemoryTable.session_id))
    .where(eq(SessionMemoryTable.session_id, sessionID))
    .get()
    .pipe(Effect.orDie)
  return row satisfies MemoryEntry | undefined
})

export const loadProject = Effect.fn("SessionMemory.loadProject")(function* (
  db: DatabaseService,
  projectID: ProjectV2.ID,
  excludeSessionID?: SessionSchema.ID,
) {
  const rows = yield* db
    .select({
      sessionID: SessionMemoryTable.session_id,
      title: SessionTable.title,
      summary: SessionMemoryTable.summary,
      recent: SessionMemoryTable.recent,
      timeUpdated: SessionMemoryTable.time_updated,
    })
    .from(SessionMemoryTable)
    .innerJoin(SessionTable, eq(SessionTable.id, SessionMemoryTable.session_id))
    .where(
      excludeSessionID
        ? and(eq(SessionMemoryTable.project_id, projectID), ne(SessionMemoryTable.session_id, excludeSessionID))
        : eq(SessionMemoryTable.project_id, projectID),
    )
    .orderBy(desc(SessionMemoryTable.time_updated))
    .limit(PROJECT_MEMORY_LIMIT)
    .all()
    .pipe(Effect.orDie)
  return rows satisfies MemoryEntry[]
})

export const renderForScope = Effect.fn("SessionMemory.renderForScope")(function* (
  db: DatabaseService,
  input: {
    readonly sessionID: SessionSchema.ID
    readonly projectID: ProjectV2.ID
    readonly scope: Scope
  },
) {
  if (input.scope === "none") return undefined
  const includeSession = input.scope === "session" || input.scope === "session-project"
  const includeProject = input.scope === "project" || input.scope === "session-project"
  const sessionMemory = includeSession ? yield* loadSession(db, input.sessionID) : undefined
  const projectMemory = includeProject
    ? yield* loadProject(db, input.projectID, includeSession ? input.sessionID : undefined)
    : []
  const rendered = [
    sessionMemory ? renderSessionMemory(sessionMemory) : undefined,
    renderProjectMemory(projectMemory),
  ].filter((item): item is string => item !== undefined)
  return rendered.length ? rendered.join("\n\n") : undefined
})

export const projectContextText = Effect.fn("SessionMemory.projectContextText")(function* () {
  const location = yield* Location.Service
  const db = (yield* Database.Service).db
  return renderProjectMemory(yield* loadProject(db, location.project.id))
})

const projectContext = Effect.fn("SessionMemory.projectContext")(function* (db: DatabaseService, projectID: ProjectV2.ID) {
  const entries = (yield* loadProject(db, projectID)).map(toSnapshotEntry)
  if (entries.length === 0) return SystemContext.empty
  return SystemContext.make({
    key: PROJECT_CONTEXT_KEY,
    codec: Schema.toCodecJson(Schema.Array(ProjectSnapshotEntry)),
    load: Effect.succeed(entries),
    baseline: (entries) => renderProjectMemory(entries) ?? "Project memory is empty.",
    update: (_previous, entries) => renderProjectMemory(entries) ?? "Project memory is empty.",
    removed: () => "Project memory is currently empty; disregard the previous project memory section.",
  })
})

export const locationLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const registry = yield* SystemContextRegistry.Service
    const location = yield* Location.Service
    const db = (yield* Database.Service).db
    yield* registry.register({ key: PROJECT_CONTEXT_KEY, load: projectContext(db, location.project.id) })
  }),
)

function renderSessionMemory(entry: MemoryEntry) {
  return [
    "<session_memory>",
    "Use this durable memory from the parent session when it is relevant to the delegated task.",
    renderEntry(entry),
    "</session_memory>",
  ].join("\n")
}

export function renderProjectMemory(entries: readonly ProjectSnapshotEntry[]) {
  if (entries.length === 0) return undefined
  return truncateText(
    [
      "<project_memory>",
      "Recent durable memory from this project. Treat it as orientation, and verify details against the repository when precision matters.",
      ...entries.map(renderEntry),
      "</project_memory>",
    ].join("\n"),
    CONTEXT_CHAR_LIMIT,
  )
}

function renderEntry(entry: ProjectSnapshotEntry) {
  return [
    `<session id="${entry.sessionID}">`,
    `<title>${entry.title}</title>`,
    `<updated>${new Date(entry.timeUpdated).toISOString()}</updated>`,
    "<summary>",
    truncateText(entry.summary.trim(), ENTRY_CHAR_LIMIT),
    "</summary>",
    "</session>",
  ].join("\n")
}

function toSnapshotEntry(entry: MemoryEntry): ProjectSnapshotEntry {
  return {
    sessionID: entry.sessionID,
    title: entry.title,
    summary: truncateText(entry.summary.trim(), ENTRY_CHAR_LIMIT),
    timeUpdated: entry.timeUpdated,
  }
}

function truncateText(value: string, limit: number) {
  return value.length <= limit ? value : `${value.slice(0, limit)}\n[truncated]`
}
