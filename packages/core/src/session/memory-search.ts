export * as SessionMemorySearch from "./memory-search"

import { sql } from "drizzle-orm"
import { Context, Effect, Layer, Schema } from "effect"
import fs from "node:fs/promises"
import path from "node:path"
import { Database } from "../database/database"
import { Global } from "../global"
import { ProjectV2 } from "../project"
import { SessionSchema } from "./schema"
import { MemoryFtsTable } from "./sql"

const SCORE_FLOOR_RATIO = 0.15

type Scope = "global" | "projects" | "sessions" | "cc"
type WritableScope = Exclude<Scope, "cc">
type MemoryType = "free" | "memory" | "checkpoint" | "progress" | "notes" | "feedback" | "project" | "reference" | "user"
type DatabaseService = Database.Interface["db"]

type MemoryLocator = {
  readonly scope: Scope
  readonly scopeID: string
  readonly type: MemoryType
  readonly key: string
}

type SearchRow = {
  readonly path: string
  readonly scope: string
  readonly scope_id: string
  readonly type: string
  readonly snippet: string
  readonly score: number
}

export const SearchResult = Schema.Struct({
  path: Schema.String,
  snippet: Schema.String,
  score: Schema.Finite,
  scope: Schema.String,
  scopeID: Schema.String,
  type: Schema.String,
}).annotate({ identifier: "SessionMemorySearch.SearchResult" })
export type SearchResult = typeof SearchResult.Type

export interface Interface {
  readonly root: () => Effect.Effect<string>
  readonly reconcile: () => Effect.Effect<{ readonly indexed: number; readonly pruned: number }>
  readonly write: (input: {
    readonly scope: WritableScope
    readonly scopeID?: string
    readonly key: string
    readonly body: string
    readonly mode?: "replace" | "append"
  }) => Effect.Effect<{
    readonly path: string
    readonly scope: WritableScope
    readonly scopeID: string
    readonly key: string
    readonly type: MemoryType
    readonly existed: boolean
  }>
  readonly search: (input: {
    readonly query: string
    readonly scope?: Scope
    readonly scopeID?: string
    readonly type?: string
    readonly limit?: number
  }) => Effect.Effect<ReadonlyArray<SearchResult>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SessionMemorySearch") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const global = yield* Global.Service
    const root = path.join(global.data, "memory")

    const reconcile = Effect.fn("SessionMemorySearch.reconcile")(function* () {
      return yield* reconcileMemory(db, root, global.home).pipe(Effect.orDie)
    })

    const write: Interface["write"] = Effect.fn("SessionMemorySearch.write")(function* (input) {
      const result = yield* writeMemory(db, root, input).pipe(Effect.orDie)
      return result
    })

    const search = Effect.fn("SessionMemorySearch.search")(function* (input: {
      readonly query: string
      readonly scope?: Scope
      readonly scopeID?: string
      readonly type?: string
      readonly limit?: number
    }) {
      yield* reconcile()
      const ftsQuery = buildFtsQuery(input.query)
      if (!ftsQuery) return []
      const limit = input.limit ?? 10
      const filters = [
        input.scope ? sql`memory_fts.scope = ${input.scope}` : undefined,
        input.scopeID ? sql`memory_fts.scope_id = ${input.scopeID}` : undefined,
        input.type ? sql`memory_fts.type = ${input.type}` : undefined,
      ].filter((filter) => filter !== undefined)
      const rows = yield* db
        .all<SearchRow>(sql`
          SELECT memory_fts.path, memory_fts.scope, memory_fts.scope_id, memory_fts.type,
                 snippet(memory_fts_idx, 0, '<<', '>>', '...', 32) AS snippet,
                 bm25(memory_fts_idx) AS score
          FROM memory_fts_idx
          JOIN memory_fts ON memory_fts.id = memory_fts_idx.rowid
          WHERE memory_fts_idx MATCH ${ftsQuery}
          ${filters.length ? sql`AND ${sql.join(filters, sql` AND `)}` : sql``}
          ORDER BY score
          LIMIT ${Math.min(limit * 3, 50)}
        `)
        .pipe(Effect.orDie)
      const mapped = rows.map((row) => ({
        path: row.path,
        snippet: row.snippet,
        score: -row.score,
        scope: row.scope,
        scopeID: row.scope_id,
        type: row.type,
      }))
      if (mapped.length === 0) return []
      const cutoff = mapped[0].score * SCORE_FLOOR_RATIO
      return mapped.filter((row, index) => index === 0 || row.score >= cutoff).slice(0, limit)
    })

    return Service.of({
      root: () => Effect.succeed(root),
      reconcile,
      write,
      search,
    })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(Database.defaultLayer), Layer.provide(Global.defaultLayer))

export const writeCompactionFiles = Effect.fn("SessionMemorySearch.writeCompactionFiles")(function* (
  db: DatabaseService,
  input: {
    readonly root: string
    readonly sessionID: SessionSchema.ID
    readonly projectID: ProjectV2.ID
    readonly sourceMessageID: string
    readonly summary: string
    readonly recent: string
    readonly timeUpdated: number
  },
) {
  const body = [
    "---",
    "type: checkpoint",
    `session: ${input.sessionID}`,
    `project: ${input.projectID}`,
    `source_message: ${input.sourceMessageID}`,
    `updated: ${new Date(input.timeUpdated).toISOString()}`,
    "---",
    "",
    "# Session Checkpoint",
    "",
    "## Summary",
    "",
    input.summary.trim(),
    "",
    "## Recent Context",
    "",
    input.recent.trim(),
    "",
  ].join("\n")
  const sessionFile = buildPath({ root: input.root, scope: "sessions", scopeID: input.sessionID, key: "checkpoint" })
  const projectFile = buildPath({
    root: input.root,
    scope: "projects",
    scopeID: input.projectID,
    key: `sessions/${input.sessionID}`,
  })
  yield* Effect.promise(() => fs.mkdir(path.dirname(sessionFile), { recursive: true }))
  yield* Effect.promise(() => fs.mkdir(path.dirname(projectFile), { recursive: true }))
  yield* Effect.promise(() => Bun.write(sessionFile, body))
  yield* Effect.promise(() => Bun.write(projectFile, body))
  yield* indexFile(db, sessionFile)
  yield* indexFile(db, projectFile)
})

function buildFtsQuery(raw: string) {
  const tokens =
    raw
      .match(/[\p{L}\p{N}_]+/gu)
      ?.map((token) => token.trim())
      .filter(Boolean) ?? []
  if (tokens.length === 0) return
  return tokens.map((token) => `"${token.replaceAll('"', "")}"`).join(" OR ")
}

async function walkMemoryDir(root: string) {
  const out: string[] = []
  async function recurse(dir: string): Promise<void> {
    const entries = await fs.readdir(dir, { withFileTypes: true }).catch((error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") return []
      throw error
    })
    for (const entry of entries) {
      const full = path.join(dir, entry.name)
      if (entry.isDirectory()) await recurse(full)
      if (entry.isFile() && full.endsWith(".md")) out.push(full)
    }
  }
  await recurse(root)
  return out
}

async function walkClaudeCodeRoot(home: string) {
  const base = path.join(home, ".claude", "projects")
  const slugs = await fs.readdir(base, { withFileTypes: true }).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return []
    throw error
  })
  const out: string[] = []
  for (const entry of slugs) {
    if (!entry.isDirectory()) continue
    out.push(...(await walkMemoryDir(path.join(base, entry.name, "memory"))))
  }
  return out
}

const reconcileMemory = Effect.fn("SessionMemorySearch.reconcileMemory")(function* (
  db: DatabaseService,
  root: string,
  home: string,
) {
  const files = new Set([
    ...(yield* Effect.promise(() => walkMemoryDir(root))),
    ...(yield* Effect.promise(() => walkClaudeCodeRoot(home))),
  ])
  const indexed = yield* db
    .select({ path: MemoryFtsTable.path, fingerprint: MemoryFtsTable.fingerprint })
    .from(MemoryFtsTable)
    .all()
    .pipe(Effect.orDie)
  let pruned = 0
  for (const row of indexed) {
    if (files.has(row.path)) continue
    yield* db.delete(MemoryFtsTable).where(sql`${MemoryFtsTable.path} = ${row.path}`).run().pipe(Effect.orDie)
    pruned++
  }
  let indexedCount = 0
  const fingerprints = new Map(indexed.map((row) => [row.path, row.fingerprint]))
  for (const file of files) {
    const result = yield* indexFile(db, file, fingerprints.get(file))
    if (result === "updated") indexedCount++
  }
  return { indexed: indexedCount, pruned }
})

const indexFile = Effect.fn("SessionMemorySearch.indexFile")(function* (
  db: DatabaseService,
  file: string,
  oldFingerprint?: string,
) {
  const locator = parsePath(file) ?? parseClaudeCodePath(file)
  if (!locator) return "skipped" as const
  const exists = yield* Effect.promise(() => Bun.file(file).exists())
  if (!exists) return "skipped" as const
  const stat = yield* Effect.promise(() => fs.stat(file))
  const fingerprint = `${stat.size}-${stat.mtimeMs}`
  if (oldFingerprint === fingerprint) return "hit" as const
  const body = yield* Effect.promise(() => Bun.file(file).text())
  yield* db
    .insert(MemoryFtsTable)
    .values({
      path: file,
      scope: locator.scope,
      scope_id: locator.scopeID,
      type: locator.scope === "cc" ? parseClaudeCodeType(body) ?? "free" : locator.type,
      body,
      fingerprint,
      last_indexed_at: Date.now(),
    })
    .onConflictDoUpdate({
      target: MemoryFtsTable.path,
      set: {
        scope: locator.scope,
        scope_id: locator.scopeID,
        type: locator.scope === "cc" ? parseClaudeCodeType(body) ?? "free" : locator.type,
        body,
        fingerprint,
        last_indexed_at: Date.now(),
      },
    })
    .run()
    .pipe(Effect.orDie)
  return "updated" as const
})

const writeMemory = Effect.fn("SessionMemorySearch.writeMemory")(function* (
  db: DatabaseService,
  root: string,
  input: {
    readonly scope: WritableScope
    readonly scopeID?: string
    readonly key: string
    readonly body: string
    readonly mode?: "replace" | "append"
  },
) {
  if (input.scope !== "global" && !input.scopeID?.trim())
    return yield* Effect.die(`Memory scope ${input.scope} requires scopeID`)
  const file = buildPath({ root, scope: input.scope, scopeID: input.scopeID, key: input.key })
  const existed = yield* Effect.promise(() => Bun.file(file).exists())
  const body =
    input.mode === "append" && existed
      ? appendMarkdown(yield* Effect.promise(() => Bun.file(file).text()), input.body)
      : ensureTrailingNewline(input.body)
  yield* Effect.promise(() => fs.mkdir(path.dirname(file), { recursive: true }))
  yield* Effect.promise(() => Bun.write(file, body))
  yield* indexFile(db, file)
  return {
    path: file,
    scope: input.scope,
    scopeID: input.scope === "global" ? "" : input.scopeID ?? "",
    key: input.key,
    type: detectType(input.key),
    existed,
  }
})

function parsePath(file: string): MemoryLocator | undefined {
  const match = normalize(file).match(/\/memory\/(global|projects|sessions)(?:\/([^/]+))?\/(.+)\.md$/)
  if (!match) return
  return {
    scope: match[1] as Scope,
    scopeID: match[1] === "global" ? "" : (match[2] ?? ""),
    type: detectType(match[3]),
    key: match[3],
  }
}

function parseClaudeCodePath(file: string): MemoryLocator | undefined {
  const match = normalize(file).match(/\/\.claude\/projects\/([^/]+)\/memory\/(.+)\.md$/)
  if (!match) return
  return { scope: "cc", scopeID: match[1], type: "free", key: match[2] }
}

function parseClaudeCodeType(body: string): MemoryType | undefined {
  const value = /^---\n([\s\S]*?)\n---\n/.exec(body)?.[1]?.match(/^[ \t]*type:[ \t]*(\w+)[ \t]*$/m)?.[1]
  return value === "feedback" || value === "project" || value === "reference" || value === "user" ? value : undefined
}

function detectType(key: string): MemoryType {
  if (/^memory$/i.test(key) || /^memory-/i.test(key)) return "memory"
  if (/^checkpoint$/.test(key) || /^checkpoint-/.test(key)) return "checkpoint"
  if (/^tasks\/[^/]+\/progress$/.test(key)) return "progress"
  if (/^tasks\/[^/]+\/notes$/.test(key)) return "notes"
  if (/^sessions\/[^/]+$/.test(key)) return "checkpoint"
  return "free"
}

function buildPath(input: { readonly root: string; readonly scope: Scope; readonly scopeID?: string; readonly key: string }) {
  if (input.scopeID !== undefined) assertSafeComponent(input.scopeID)
  assertSafeComponent(input.key)
  return path.join(input.root, input.scope, ...(input.scope === "global" ? [] : [input.scopeID ?? ""]), `${input.key}.md`)
}

function appendMarkdown(current: string, next: string) {
  const prefix = current.trimEnd()
  const suffix = next.trim()
  if (!prefix) return ensureTrailingNewline(suffix)
  if (!suffix) return ensureTrailingNewline(prefix)
  return `${prefix}\n\n${suffix}\n`
}

function ensureTrailingNewline(value: string) {
  return value.endsWith("\n") ? value : `${value}\n`
}

function assertSafeComponent(value: string) {
  if (value.startsWith("/")) throw new Error(`Invalid memory path component: ${value}`)
  for (const segment of value.split("/")) {
    if (segment === ".." || segment.length === 0) throw new Error(`Invalid memory path component: ${value}`)
  }
}

function normalize(value: string) {
  return value.split(path.sep).join("/")
}
