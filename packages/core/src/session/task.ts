export * as SessionTask from "./task"

import { and, asc, eq, gt, isNull, or, type SQL } from "drizzle-orm"
import { Context, Effect, Layer, Schema } from "effect"
import { Database } from "../database/database"
import { SessionSchema } from "./schema"
import {
  SessionTaskEventTable,
  SessionTaskTable,
  type SessionTaskEventKind,
  type SessionTaskStatus,
} from "./sql"

const DAY_MS = 24 * 60 * 60 * 1_000
const CLEANUP_DAYS = 7

export const Status = Schema.Literals(["open", "in_progress", "blocked", "done", "abandoned"]).annotate({
  identifier: "SessionTask.Status",
})
export type Status = typeof Status.Type

export const EventKind = Schema.Literals([
  "created",
  "started",
  "unstarted",
  "blocked",
  "unblocked",
  "done",
  "abandoned",
  "renamed",
]).annotate({ identifier: "SessionTask.EventKind" })
export type EventKind = typeof EventKind.Type

export const Info = Schema.Struct({
  id: Schema.String.annotate({ description: "Task id, for example T1 or T1.1" }),
  sessionID: SessionSchema.ID,
  parentTaskID: Schema.String.pipe(Schema.optional),
  status: Status,
  summary: Schema.String,
  owner: Schema.String.pipe(Schema.optional),
  timeCreated: Schema.Finite,
  timeUpdated: Schema.Finite,
  timeEnded: Schema.Finite.pipe(Schema.optional),
  timeCleanup: Schema.Finite.pipe(Schema.optional),
}).annotate({ identifier: "SessionTask.Info" })
export type Info = typeof Info.Type

export const EventInfo = Schema.Struct({
  id: Schema.Finite,
  taskID: Schema.String,
  at: Schema.Finite,
  kind: EventKind,
  summary: Schema.String.pipe(Schema.optional),
}).annotate({ identifier: "SessionTask.EventInfo" })
export type EventInfo = typeof EventInfo.Type

type DatabaseService = Database.Interface["db"]

export interface Interface {
  readonly create: (input: {
    readonly sessionID: SessionSchema.ID
    readonly summary: string
    readonly parentTaskID?: string
    readonly owner?: string
  }) => Effect.Effect<Info>
  readonly list: (input: {
    readonly sessionID?: SessionSchema.ID
    readonly status?: Status
    readonly owner?: string
    readonly includeTerminal?: boolean
    readonly includeArchived?: boolean
  }) => Effect.Effect<ReadonlyArray<Info>>
  readonly get: (input: { readonly sessionID: SessionSchema.ID; readonly id: string }) => Effect.Effect<Info | undefined>
  readonly start: (input: {
    readonly sessionID: SessionSchema.ID
    readonly id: string
    readonly owner?: string
    readonly eventSummary?: string
  }) => Effect.Effect<Info>
  readonly block: (input: {
    readonly sessionID: SessionSchema.ID
    readonly id: string
    readonly eventSummary?: string
  }) => Effect.Effect<Info>
  readonly unblock: (input: {
    readonly sessionID: SessionSchema.ID
    readonly id: string
    readonly eventSummary?: string
  }) => Effect.Effect<Info>
  readonly done: (input: {
    readonly sessionID: SessionSchema.ID
    readonly id: string
    readonly eventSummary?: string
  }) => Effect.Effect<Info>
  readonly abandon: (input: {
    readonly sessionID: SessionSchema.ID
    readonly id: string
    readonly eventSummary?: string
  }) => Effect.Effect<Info>
  readonly rename: (input: {
    readonly sessionID: SessionSchema.ID
    readonly id: string
    readonly summary: string
  }) => Effect.Effect<Info>
  readonly events: (input: {
    readonly sessionID: SessionSchema.ID
    readonly taskID: string
  }) => Effect.Effect<ReadonlyArray<EventInfo>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SessionTask") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service

    const get = Effect.fn("SessionTask.get")(function* (input: { readonly sessionID: SessionSchema.ID; readonly id: string }) {
      const row = yield* db
        .select()
        .from(SessionTaskTable)
        .where(and(eq(SessionTaskTable.session_id, input.sessionID), eq(SessionTaskTable.id, input.id)))
        .get()
        .pipe(Effect.orDie)
      return row ? fromTaskRow(row) : undefined
    })

    const create = Effect.fn("SessionTask.create")(function* (input: {
      readonly sessionID: SessionSchema.ID
      readonly summary: string
      readonly parentTaskID?: string
      readonly owner?: string
    }) {
      return yield* db
        .transaction((tx) =>
          Effect.gen(function* () {
            const siblings = yield* tx
              .select({ id: SessionTaskTable.id })
              .from(SessionTaskTable)
              .where(
                and(
                  eq(SessionTaskTable.session_id, input.sessionID),
                  input.parentTaskID
                    ? eq(SessionTaskTable.parent_task_id, input.parentTaskID)
                    : isNull(SessionTaskTable.parent_task_id),
                ),
              )
              .all()
            const now = Date.now()
            const row = {
              session_id: input.sessionID,
              id: nextChildID(
                input.parentTaskID,
                siblings.map((sibling) => sibling.id),
              ),
              parent_task_id: input.parentTaskID ?? null,
              status: "open" as const,
              summary: input.summary,
              owner: input.owner ?? null,
              time_created: now,
              time_updated: now,
              time_ended: null,
              time_cleanup: null,
            }
            yield* tx.insert(SessionTaskTable).values(row).run()
            yield* insertEvent(tx, input.sessionID, row.id, "created", undefined, now)
            return fromTaskRow(row)
          }),
        )
        .pipe(Effect.orDie)
    })

    const list = Effect.fn("SessionTask.list")(function* (input: {
      readonly sessionID?: SessionSchema.ID
      readonly status?: Status
      readonly owner?: string
      readonly includeTerminal?: boolean
      readonly includeArchived?: boolean
    }) {
      const conditions: SQL[] = []
      if (input.sessionID) conditions.push(eq(SessionTaskTable.session_id, input.sessionID))
      if (input.status) conditions.push(eq(SessionTaskTable.status, input.status))
      if (input.owner) conditions.push(eq(SessionTaskTable.owner, input.owner))
      if (input.includeTerminal !== true) {
        const nonTerminal = or(
          eq(SessionTaskTable.status, "open"),
          eq(SessionTaskTable.status, "in_progress"),
          eq(SessionTaskTable.status, "blocked"),
        )
        if (nonTerminal) conditions.push(nonTerminal)
      }
      if (input.includeArchived !== true) {
        const notArchived = or(isNull(SessionTaskTable.time_cleanup), gt(SessionTaskTable.time_cleanup, Date.now()))
        if (notArchived) conditions.push(notArchived)
      }
      const rows = yield* db
        .select()
        .from(SessionTaskTable)
        .where(conditions.length ? and(...conditions) : undefined)
        .orderBy(asc(SessionTaskTable.time_created), asc(SessionTaskTable.id))
        .all()
        .pipe(Effect.orDie)
      return rows.map(fromTaskRow)
    })

    const events = Effect.fn("SessionTask.events")(function* (input: {
      readonly sessionID: SessionSchema.ID
      readonly taskID: string
    }) {
      const rows = yield* db
        .select()
        .from(SessionTaskEventTable)
        .where(and(eq(SessionTaskEventTable.session_id, input.sessionID), eq(SessionTaskEventTable.task_id, input.taskID)))
        .orderBy(asc(SessionTaskEventTable.at), asc(SessionTaskEventTable.id))
        .all()
        .pipe(Effect.orDie)
      return rows.map(fromEventRow)
    })

    const transition = Effect.fn("SessionTask.transition")(function* (input: {
      readonly sessionID: SessionSchema.ID
      readonly id: string
      readonly status: SessionTaskStatus
      readonly kind: SessionTaskEventKind
      readonly eventSummary?: string
      readonly owner?: string
      readonly terminal?: boolean
    }) {
      const current = yield* get({ sessionID: input.sessionID, id: input.id })
      if (!current) return yield* Effect.die(`Task ${input.id} not found in session ${input.sessionID}`)
      if (current.status === "done" || current.status === "abandoned") return current
      const now = Date.now()
      yield* db
        .transaction((tx) =>
          Effect.gen(function* () {
            yield* tx
              .update(SessionTaskTable)
              .set({
                status: input.status,
                owner: input.owner ?? current.owner ?? null,
                time_updated: now,
                time_ended: input.terminal ? now : null,
                time_cleanup: input.terminal ? now + CLEANUP_DAYS * DAY_MS : null,
              })
              .where(and(eq(SessionTaskTable.session_id, input.sessionID), eq(SessionTaskTable.id, input.id)))
              .run()
            yield* insertEvent(tx, input.sessionID, input.id, input.kind, input.eventSummary, now)
          }),
        )
        .pipe(Effect.orDie)
      const updated = yield* get({ sessionID: input.sessionID, id: input.id })
      if (!updated) return yield* Effect.die(`Task ${input.id} not found in session ${input.sessionID}`)
      return updated
    })

    const rename = Effect.fn("SessionTask.rename")(function* (input: {
      readonly sessionID: SessionSchema.ID
      readonly id: string
      readonly summary: string
    }) {
      const current = yield* get({ sessionID: input.sessionID, id: input.id })
      if (!current) return yield* Effect.die(`Task ${input.id} not found in session ${input.sessionID}`)
      const now = Date.now()
      yield* db
        .transaction((tx) =>
          Effect.gen(function* () {
            yield* tx
              .update(SessionTaskTable)
              .set({ summary: input.summary, time_updated: now })
              .where(and(eq(SessionTaskTable.session_id, input.sessionID), eq(SessionTaskTable.id, input.id)))
              .run()
            yield* insertEvent(tx, input.sessionID, input.id, "renamed", input.summary, now)
          }),
        )
        .pipe(Effect.orDie)
      const updated = yield* get({ sessionID: input.sessionID, id: input.id })
      if (!updated) return yield* Effect.die(`Task ${input.id} not found in session ${input.sessionID}`)
      return updated
    })

    return Service.of({
      create,
      list,
      get,
      events,
      start: (input) =>
        transition({
          sessionID: input.sessionID,
          id: input.id,
          status: "in_progress",
          kind: "started",
          owner: input.owner,
          eventSummary: input.eventSummary,
        }),
      block: (input) =>
        transition({
          sessionID: input.sessionID,
          id: input.id,
          status: "blocked",
          kind: "blocked",
          eventSummary: input.eventSummary,
        }),
      unblock: (input) =>
        transition({
          sessionID: input.sessionID,
          id: input.id,
          status: "open",
          kind: "unblocked",
          eventSummary: input.eventSummary,
        }),
      done: (input) =>
        transition({
          sessionID: input.sessionID,
          id: input.id,
          status: "done",
          kind: "done",
          eventSummary: input.eventSummary,
          terminal: true,
        }),
      abandon: (input) =>
        transition({
          sessionID: input.sessionID,
          id: input.id,
          status: "abandoned",
          kind: "abandoned",
          eventSummary: input.eventSummary,
          terminal: true,
        }),
      rename,
    })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(Database.defaultLayer))

function nextChildID(parentTaskID: string | undefined, siblings: readonly string[]) {
  const prefix = parentTaskID ? `${parentTaskID}.` : "T"
  const used = siblings
    .filter((sibling) => (parentTaskID ? sibling.startsWith(prefix) : /^T\d+$/.test(sibling)))
    .map((sibling) => {
      const tail = sibling.slice(prefix.length)
      return /^\d+$/.test(tail) ? Number(tail) : 0
    })
  return `${prefix}${used.length ? Math.max(...used) + 1 : 1}`
}

function fromTaskRow(row: typeof SessionTaskTable.$inferSelect): Info {
  return {
    id: row.id,
    sessionID: row.session_id,
    parentTaskID: row.parent_task_id ?? undefined,
    status: row.status,
    summary: row.summary,
    owner: row.owner ?? undefined,
    timeCreated: row.time_created,
    timeUpdated: row.time_updated,
    timeEnded: row.time_ended ?? undefined,
    timeCleanup: row.time_cleanup ?? undefined,
  }
}

function fromEventRow(row: typeof SessionTaskEventTable.$inferSelect): EventInfo {
  return {
    id: row.id,
    taskID: row.task_id,
    at: row.at,
    kind: row.kind,
    summary: row.summary ?? undefined,
  }
}

function insertEvent(
  db: Pick<DatabaseService, "insert">,
  sessionID: SessionSchema.ID,
  taskID: string,
  kind: SessionTaskEventKind,
  summary: string | undefined,
  at: number,
) {
  return db
    .insert(SessionTaskEventTable)
    .values({ session_id: sessionID, task_id: taskID, kind, summary: summary ?? null, at })
    .run()
}
