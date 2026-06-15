export * as WorkflowRuntime from "./runtime"

import { createHash } from "node:crypto"
import { createContext, Script } from "node:vm"
import fs from "node:fs/promises"
import path from "node:path"
import { and, desc, eq } from "drizzle-orm"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Scope } from "effect"
import { Database } from "../database/database"
import { Global } from "../global"
import { Identifier } from "../id/id"
import { Location } from "../location"
import { SessionSchema } from "../session/schema"
import { WorkflowAgent } from "./agent"
import { WorkflowRunTable, type WorkflowRunStatus } from "./sql"
import { WorkflowWorkspace } from "./workspace"

const DEFAULT_MAX_CONCURRENT_AGENTS = 8
const DEFAULT_SCRIPT_TIMEOUT_MS = 10 * 60 * 1_000
const RUN_ID = /^wf_[0-9A-Za-z]+$/

type DatabaseService = Database.Interface["db"]
type Active = {
  readonly done: Deferred.Deferred<RunOutcome>
  readonly scope: Scope.Closeable
  fiber?: Fiber.Fiber<void, never>
}

export type AgentInput = {
  readonly runID: string
  readonly sessionID: SessionSchema.ID
  readonly prompt: string
  readonly options: Record<string, unknown>
  readonly workspace?: string
}

export type RunOutcome =
  | { readonly status: "completed"; readonly result: unknown }
  | { readonly status: "failed"; readonly error: string }
  | { readonly status: "cancelled" }

export type WorktreeInput =
  | boolean
  | {
      readonly sourceDirectory?: string
      readonly parentDirectory?: string
      readonly name?: string
      readonly removeOnFinish?: boolean
      readonly removeOnCancel?: boolean
      readonly forceRemove?: boolean
    }

export const Summary = Schema.Struct({
  runID: Schema.String,
  sessionID: SessionSchema.ID,
  name: Schema.String,
  status: Schema.Literals(["running", "completed", "failed", "cancelled"]),
  workspace: Schema.String.pipe(Schema.optional),
  workspaceManaged: Schema.Boolean,
  workspaceRemoveOnFinish: Schema.Boolean,
  workspaceRemoveOnCancel: Schema.Boolean,
  workspaceForceRemove: Schema.Boolean,
  scriptSha: Schema.String,
  running: Schema.Finite,
  succeeded: Schema.Finite,
  failed: Schema.Finite,
  currentPhase: Schema.String.pipe(Schema.optional),
  args: Schema.Unknown.pipe(Schema.optional),
  result: Schema.Unknown.pipe(Schema.optional),
  error: Schema.String.pipe(Schema.optional),
  timeCreated: Schema.Finite,
  timeUpdated: Schema.Finite,
}).annotate({ identifier: "WorkflowRuntime.Summary" })
export type Summary = typeof Summary.Type

export interface Interface {
  readonly start: (input: {
    readonly runID?: string
    readonly sessionID: SessionSchema.ID
    readonly name?: string
    readonly script: string
    readonly args?: unknown
    readonly workspace?: string
    readonly worktree?: WorktreeInput
    readonly maxConcurrentAgents?: number
    readonly scriptTimeoutMs?: number
    readonly agent?: (input: AgentInput) => Effect.Effect<unknown, unknown>
  }) => Effect.Effect<Summary>
  readonly resume: (input: {
    readonly runID: string
    readonly agent?: (input: AgentInput) => Effect.Effect<unknown, unknown>
    readonly maxConcurrentAgents?: number
    readonly scriptTimeoutMs?: number
  }) => Effect.Effect<{ readonly runID: string; readonly resumed: boolean }>
  readonly status: (runID: string) => Effect.Effect<Summary | undefined>
  readonly list: (input?: { readonly sessionID?: SessionSchema.ID }) => Effect.Effect<Summary[]>
  readonly wait: (input: { readonly runID: string; readonly timeoutMs?: number }) => Effect.Effect<RunOutcome | undefined>
  readonly cancel: (runID: string) => Effect.Effect<Summary | undefined>
  readonly reclaim: (runID: string) => Effect.Effect<Summary | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/WorkflowRuntime") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const global = yield* Global.Service
    const location = yield* Effect.serviceOption(Location.Service)
    const workflowAgent = Option.getOrUndefined(yield* Effect.serviceOption(WorkflowAgent.Service))
    const workflowWorkspace = Option.getOrUndefined(yield* Effect.serviceOption(WorkflowWorkspace.Service))
    const root = path.join(global.data, "workflow")
    const scope = yield* Effect.scope
    const active = new Map<string, Active>()

    const start: Interface["start"] = Effect.fn("WorkflowRuntime.start")(function* (input) {
      const runID = validateRunID(input.runID ?? Identifier.create("wf", "ascending"))
      const current = active.get(runID)
      if (current) {
        const running = yield* load(db, runID)
        if (running) return running
      }
      const scriptSha = hash(input.script)
      const lease = yield* createWorktreeLease(workflowWorkspace, runID, input.worktree)
      const workspace = lease?.directory ?? input.workspace ?? (location._tag === "Some" ? location.value.directory : undefined)
      const now = Date.now()
      yield* writeScript(root, runID, input.script)
      yield* clearJournal(root, runID)
      yield* db
        .insert(WorkflowRunTable)
        .values({
          id: runID,
          session_id: input.sessionID,
          name: input.name ?? "workflow",
          status: "running",
          workspace,
          workspace_managed: lease?.managed ?? false,
          workspace_remove_on_finish: lease?.removeOnFinish ?? false,
          workspace_remove_on_cancel: lease?.removeOnCancel ?? false,
          workspace_force_remove: lease?.forceRemove ?? true,
          script_sha: scriptSha,
          running: 0,
          succeeded: 0,
          failed: 0,
          args: input.args,
          time_created: now,
          time_updated: now,
        })
        .onConflictDoUpdate({
          target: WorkflowRunTable.id,
          set: {
            session_id: input.sessionID,
            name: input.name ?? "workflow",
            status: "running",
            workspace,
            workspace_managed: lease?.managed ?? false,
            workspace_remove_on_finish: lease?.removeOnFinish ?? false,
            workspace_remove_on_cancel: lease?.removeOnCancel ?? false,
            workspace_force_remove: lease?.forceRemove ?? true,
            script_sha: scriptSha,
            running: 0,
            succeeded: 0,
            failed: 0,
            current_phase: null,
            args: input.args,
            result: null,
            error: null,
            time_updated: now,
          },
        })
        .run()
        .pipe(Effect.orDie)
      yield* launch({
        db,
        root,
        active,
        scope,
        workspace: workflowWorkspace,
        input: {
          runID,
          sessionID: input.sessionID,
          name: input.name ?? "workflow",
          script: input.script,
          args: input.args,
          workspace,
          maxConcurrentAgents: input.maxConcurrentAgents,
          scriptTimeoutMs: input.scriptTimeoutMs,
          agent: input.agent ?? workflowAgent?.run,
          replayJournal: false,
        },
      })
      const summary = yield* load(db, runID)
      if (!summary) return yield* Effect.die(`Workflow run ${runID} was not recorded`)
      return summary
    })

    const resume: Interface["resume"] = Effect.fn("WorkflowRuntime.resume")(function* (input) {
      const runID = validateRunID(input.runID)
      if (active.has(runID)) return { runID, resumed: false }
      const summary = yield* load(db, runID)
      if (!summary) return { runID, resumed: false }
      if (summary.status === "completed") return { runID, resumed: false }
      const script = yield* readScript(root, runID).pipe(Effect.orElseSucceed(() => undefined))
      if (script === undefined) return { runID, resumed: false }
      yield* markRunning(db, runID)
      yield* launch({
        db,
        root,
        active,
        scope,
        workspace: workflowWorkspace,
        input: {
          runID,
          sessionID: summary.sessionID,
          name: summary.name,
          script,
          args: summary.args,
          workspace: summary.workspace,
          maxConcurrentAgents: input.maxConcurrentAgents,
          scriptTimeoutMs: input.scriptTimeoutMs,
          agent: input.agent ?? workflowAgent?.run,
          replayJournal: true,
        },
      })
      return { runID, resumed: true }
    })

    const status: Interface["status"] = Effect.fn("WorkflowRuntime.status")(function* (runID) {
      return yield* load(db, validateRunID(runID))
    })

    const list: Interface["list"] = Effect.fn("WorkflowRuntime.list")(function* (input = {}) {
      const rows = yield* db
        .select()
        .from(WorkflowRunTable)
        .where(input.sessionID ? eq(WorkflowRunTable.session_id, input.sessionID) : undefined)
        .orderBy(desc(WorkflowRunTable.time_created))
        .all()
        .pipe(Effect.orDie)
      return rows.map(toSummary)
    })

    const wait: Interface["wait"] = Effect.fn("WorkflowRuntime.wait")(function* (input) {
      const runID = validateRunID(input.runID)
      const current = active.get(runID)
      if (!current) return toOutcome(yield* load(db, runID))
      if (input.timeoutMs === undefined) return yield* Deferred.await(current.done)
      if (input.timeoutMs <= 0) return toOutcome(yield* load(db, runID))
      const outcome = yield* Deferred.await(current.done).pipe(Effect.timeoutOption(input.timeoutMs))
      return outcome._tag === "Some" ? outcome.value : toOutcome(yield* load(db, runID))
    })

    const cancel: Interface["cancel"] = Effect.fn("WorkflowRuntime.cancel")(function* (runID) {
      const id = validateRunID(runID)
      const current = active.get(id)
      if (current) {
        active.delete(id)
        yield* recordTerminal(db, id, { status: "cancelled" })
        yield* cleanupWorkspace(db, workflowWorkspace, id, "cancel")
        yield* Deferred.succeed(current.done, { status: "cancelled" }).pipe(Effect.ignore)
        if (current.fiber) yield* Fiber.interrupt(current.fiber).pipe(Effect.forkIn(scope, { startImmediately: true }), Effect.ignore)
        yield* Scope.close(current.scope, Exit.void).pipe(Effect.forkIn(scope, { startImmediately: true }), Effect.ignore)
      }
      if (!current) {
        yield* recordTerminal(db, id, { status: "cancelled" })
        yield* cleanupWorkspace(db, workflowWorkspace, id, "cancel")
      }
      return yield* load(db, id)
    })

    const reclaim: Interface["reclaim"] = Effect.fn("WorkflowRuntime.reclaim")(function* (runID) {
      const id = validateRunID(runID)
      if (active.has(id)) return yield* load(db, id)
      const summary = yield* load(db, id)
      if (summary?.status === "running") {
        yield* recordTerminal(db, id, { status: "failed", error: "Workflow run reclaimed after owner disappeared" })
        yield* cleanupWorkspace(db, workflowWorkspace, id, "cancel")
      }
      return yield* load(db, id)
    })

    return Service.of({ start, resume, status, list, wait, cancel, reclaim })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(Database.defaultLayer), Layer.provide(Global.defaultLayer))

function validateRunID(runID: string) {
  if (!RUN_ID.test(runID)) throw new Error(`Invalid workflow runID: ${runID}`)
  return runID
}

function hash(value: string) {
  return createHash("sha256").update(value).digest("hex")
}

function canonical(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value
  if (Array.isArray(value)) return value.map(canonical)
  return Object.fromEntries(
    Object.keys(value)
      .sort()
      .map((key) => [key, canonical((value as Record<string, unknown>)[key])]),
  )
}

function journalKey(prompt: string, options: Record<string, unknown>, occurrence: number) {
  return `${hash(JSON.stringify(canonical({ prompt, options })))}:${occurrence}`
}

function semaphore(max: number) {
  const queue: Array<() => void> = []
  let active = 0
  const release = () => {
    active--
    queue.shift()?.()
  }
  return <A>(run: () => Promise<A>) =>
    new Promise<A>((resolve, reject) => {
      const next = () => {
        active++
        run().then(
          (value) => {
            release()
            resolve(value)
          },
          (error) => {
            release()
            reject(error)
          },
        )
      }
      if (active < max) return next()
      queue.push(next)
    })
}

function workflowPath(root: string, runID: string, extension: "js" | "jsonl") {
  return path.join(root, `${validateRunID(runID)}.${extension}`)
}

const writeScript = (root: string, runID: string, script: string) =>
  Effect.promise(async () => {
    await fs.mkdir(root, { recursive: true })
    await Bun.write(workflowPath(root, runID, "js"), script)
  })

const readScript = (root: string, runID: string) => Effect.promise(() => Bun.file(workflowPath(root, runID, "js")).text())

const clearJournal = (root: string, runID: string) =>
  Effect.promise(async () => {
    await fs.mkdir(root, { recursive: true })
    await Bun.write(workflowPath(root, runID, "jsonl"), "")
  })

const appendJournal = (root: string, runID: string, event: unknown) =>
  Effect.promise(async () => {
    await fs.mkdir(root, { recursive: true })
    await fs.appendFile(workflowPath(root, runID, "jsonl"), `${JSON.stringify(event)}\n`)
  })

const loadJournal = Effect.fn("WorkflowRuntime.loadJournal")(function* (root: string, runID: string) {
  const text = yield* Effect.promise(() => Bun.file(workflowPath(root, runID, "jsonl")).text()).pipe(
    Effect.orElseSucceed(() => ""),
  )
  return new Map(
    text
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as unknown)
      .filter(isAgentJournal)
      .map((event) => [event.key, event.result] as const),
  )
})

function isAgentJournal(value: unknown): value is { readonly type: "agent"; readonly key: string; readonly result: unknown } {
  if (!value || typeof value !== "object") return false
  const event = value as { readonly type?: unknown; readonly key?: unknown }
  return event.type === "agent" && typeof event.key === "string"
}

const load = Effect.fn("WorkflowRuntime.load")(function* (db: DatabaseService, runID: string) {
  const row = yield* db.select().from(WorkflowRunTable).where(eq(WorkflowRunTable.id, runID)).get().pipe(Effect.orDie)
  return row ? toSummary(row) : undefined
})

const createWorktreeLease = Effect.fn("WorkflowRuntime.createWorktreeLease")(function* (
  workspace: WorkflowWorkspace.Interface | undefined,
  runID: string,
  input: WorktreeInput | undefined,
) {
  if (!input) return undefined
  if (!workspace) return yield* Effect.die("Workflow worktree requested, but no WorkflowWorkspace service is installed")
  const options = input === true ? {} : input
  return yield* workspace.create({ runID, ...options }).pipe(Effect.orDie)
})

const cleanupWorkspace = Effect.fn("WorkflowRuntime.cleanupWorkspace")(function* (
  db: DatabaseService,
  workspace: WorkflowWorkspace.Interface | undefined,
  runID: string,
  mode: "finish" | "cancel",
) {
  const summary = yield* load(db, runID)
  if (!summary?.workspaceManaged || !summary.workspace) return
  const remove = mode === "cancel" ? summary.workspaceRemoveOnCancel : summary.workspaceRemoveOnFinish
  if (!remove) return
  if (!workspace) {
    yield* Effect.logWarning("workflow managed workspace could not be released; service missing", {
      runID,
      workspace: summary.workspace,
    })
    return
  }
  yield* workspace
    .release({ directory: summary.workspace, force: summary.workspaceForceRemove })
    .pipe(Effect.catchCause((cause) => Effect.logWarning("workflow managed workspace release failed", { runID, cause })))
})

const markRunning = Effect.fn("WorkflowRuntime.markRunning")(function* (db: DatabaseService, runID: string) {
  yield* db
    .update(WorkflowRunTable)
    .set({ status: "running", error: null, time_updated: Date.now() })
    .where(eq(WorkflowRunTable.id, runID))
    .run()
    .pipe(Effect.orDie)
})

const recordCounters = Effect.fn("WorkflowRuntime.recordCounters")(function* (
  db: DatabaseService,
  input: { readonly runID: string; readonly running: number; readonly succeeded: number; readonly failed: number },
) {
  yield* db
    .update(WorkflowRunTable)
    .set({ running: input.running, succeeded: input.succeeded, failed: input.failed, time_updated: Date.now() })
    .where(and(eq(WorkflowRunTable.id, input.runID), eq(WorkflowRunTable.status, "running")))
    .run()
    .pipe(Effect.orDie)
})

const recordPhase = Effect.fn("WorkflowRuntime.recordPhase")(function* (
  db: DatabaseService,
  input: { readonly runID: string; readonly phase: string },
) {
  yield* db
    .update(WorkflowRunTable)
    .set({ current_phase: input.phase, time_updated: Date.now() })
    .where(and(eq(WorkflowRunTable.id, input.runID), eq(WorkflowRunTable.status, "running")))
    .run()
    .pipe(Effect.orDie)
})

const recordTerminal = Effect.fn("WorkflowRuntime.recordTerminal")(function* (
  db: DatabaseService,
  runID: string,
  input: RunOutcome,
) {
  yield* db
    .update(WorkflowRunTable)
    .set({
      status: input.status,
      running: 0,
      result: input.status === "completed" ? input.result : null,
      error: input.status === "failed" ? input.error : null,
      time_updated: Date.now(),
    })
    .where(eq(WorkflowRunTable.id, runID))
    .run()
    .pipe(Effect.orDie)
})

const launch = Effect.fn("WorkflowRuntime.launch")(function* (input: {
  readonly db: DatabaseService
  readonly root: string
  readonly active: Map<string, Active>
  readonly scope: Scope.Scope
  readonly workspace?: WorkflowWorkspace.Interface
  readonly input: {
    readonly runID: string
    readonly sessionID: SessionSchema.ID
    readonly name: string
    readonly script: string
    readonly args?: unknown
    readonly workspace?: string
    readonly maxConcurrentAgents?: number
    readonly scriptTimeoutMs?: number
    readonly agent?: (input: AgentInput) => Effect.Effect<unknown, unknown>
    readonly replayJournal: boolean
  }
}) {
  const done = yield* Deferred.make<RunOutcome>()
  const runScope = yield* Scope.fork(input.scope, "parallel")
  const active: Active = { done, scope: runScope }
  input.active.set(input.input.runID, active)
  active.fiber = yield* execute(input).pipe(
    Effect.exit,
    Effect.flatMap((exit) =>
      settle(input.db, input.workspace, input.input.runID, input.active, active, done, input.scope, runScope, exit),
    ),
    Effect.forkIn(runScope, { startImmediately: true }),
  )
})

const execute = Effect.fn("WorkflowRuntime.execute")(function* (input: {
  readonly db: DatabaseService
  readonly root: string
  readonly input: {
    readonly runID: string
    readonly sessionID: SessionSchema.ID
    readonly script: string
    readonly args?: unknown
    readonly workspace?: string
    readonly maxConcurrentAgents?: number
    readonly scriptTimeoutMs?: number
    readonly agent?: (input: AgentInput) => Effect.Effect<unknown, unknown>
    readonly replayJournal: boolean
  }
}) {
  const journal = input.input.replayJournal ? yield* loadJournal(input.root, input.input.runID) : new Map<string, unknown>()
  const limit = Math.max(1, input.input.maxConcurrentAgents ?? DEFAULT_MAX_CONCURRENT_AGENTS)
  const runQueued = semaphore(limit)
  let running = 0
  let succeeded = 0
  let failed = 0
  let counterWrite = Promise.resolve()
  const occurrences = new Map<string, number>()
  const runEffect = <A>(effect: Effect.Effect<A, unknown>) =>
    Effect.runPromise(effect.pipe(Effect.provideService(Database.Service, { db: input.db })))
  const writeCounters = () => {
    counterWrite = counterWrite.then(() =>
      runEffect(recordCounters(input.db, { runID: input.input.runID, running, succeeded, failed })).then(() => undefined),
    )
    return counterWrite
  }
  const hooks = {
    log: (message: unknown) =>
      runEffect(appendJournal(input.root, input.input.runID, { type: "log", message: String(message), at: Date.now() })),
    phase: (title: unknown) =>
      runEffect(
        Effect.all([
          appendJournal(input.root, input.input.runID, { type: "phase", title: String(title), at: Date.now() }),
          recordPhase(input.db, { runID: input.input.runID, phase: String(title) }),
        ]),
      ).then(() => undefined),
    agent: (prompt: unknown, options: unknown = {}) => {
      const agentOptions = isRecord(options) ? options : {}
      const keyBase = journalKey(String(prompt), agentOptions, occurrences.get(String(prompt)) ?? 0)
      occurrences.set(String(prompt), (occurrences.get(String(prompt)) ?? 0) + 1)
      if (journal.has(keyBase)) return Promise.resolve(journal.get(keyBase))
      return runQueued(async () => {
        running++
        await writeCounters()
        if (!input.input.agent) {
          running--
          failed++
          await writeCounters()
          throw new Error("Workflow agent bridge is not installed")
        }
        const exit = await Effect.runPromiseExit(
          input.input.agent({
            runID: input.input.runID,
            sessionID: input.input.sessionID,
            prompt: String(prompt),
            options: agentOptions,
            workspace: input.input.workspace,
          }),
        )
        running--
        if (Exit.isSuccess(exit)) {
          succeeded++
          await writeCounters()
          await runEffect(
            appendJournal(input.root, input.input.runID, { type: "agent", key: keyBase, result: exit.value, at: Date.now() }),
          )
          return exit.value
        }
        failed++
        await writeCounters()
        throw Cause.squash(exit.cause)
      })
    },
    ...makeFileHooks(input.input.workspace),
  }
  const result = yield* Effect.promise(() =>
    runScript(input.input.script, hooks, {
      args: input.input.args,
      workspace: input.input.workspace,
      timeoutMs: input.input.scriptTimeoutMs ?? DEFAULT_SCRIPT_TIMEOUT_MS,
    }),
  )
  yield* Effect.promise(() => counterWrite)
  return { status: "completed" as const, result: result ?? null }
})

const settle = Effect.fn("WorkflowRuntime.settle")(function* (
  db: DatabaseService,
  workspace: WorkflowWorkspace.Interface | undefined,
  runID: string,
  activeMap: Map<string, Active>,
  active: Active,
  done: Deferred.Deferred<RunOutcome>,
  ownerScope: Scope.Scope,
  scope: Scope.Closeable,
  exit: Exit.Exit<RunOutcome, unknown>,
) {
  if (activeMap.get(runID) !== active) return
  const outcome = Exit.isSuccess(exit)
    ? exit.value
    : Cause.hasInterruptsOnly(exit.cause)
      ? ({ status: "cancelled" } as const)
      : ({ status: "failed", error: String(Cause.squash(exit.cause)) } as const)
  yield* recordTerminal(db, runID, outcome)
  yield* cleanupWorkspace(db, workspace, runID, outcome.status === "cancelled" ? "cancel" : "finish")
  activeMap.delete(runID)
  yield* Deferred.succeed(done, outcome).pipe(Effect.ignore)
  yield* Scope.close(scope, Exit.void).pipe(Effect.forkIn(ownerScope, { startImmediately: true }), Effect.ignore)
})

async function runScript(
  script: string,
  hooks: {
    readonly log: (message: unknown) => Promise<unknown>
    readonly phase: (title: unknown) => Promise<unknown>
    readonly agent: (prompt: unknown, options?: unknown) => Promise<unknown>
    readonly readFile: (resource: unknown) => Promise<string | null>
    readonly writeFile: (resource: unknown, content: unknown) => Promise<void>
    readonly exists: (resource: unknown) => Promise<boolean>
    readonly glob: (pattern: unknown) => Promise<string[]>
  },
  input: { readonly args?: unknown; readonly workspace?: string; readonly timeoutMs: number },
) {
  const context = createContext(
    {
      args: input.args ?? null,
      workspace: input.workspace,
      agent: hooks.agent,
      readFile: hooks.readFile,
      writeFile: hooks.writeFile,
      exists: hooks.exists,
      glob: hooks.glob,
      log: hooks.log,
      phase: hooks.phase,
      parallel: (thunks: Array<() => unknown>) => Promise.all(thunks.map((thunk) => Promise.resolve().then(thunk))),
      pipeline: (items: unknown[], ...stages: Array<(value: unknown, item: unknown, index: number) => unknown>) =>
        Promise.all(
          items.map((item, index) =>
            stages.reduce((acc, stage) => acc.then((value) => stage(value, item, index)), Promise.resolve(item)),
          ),
        ),
    },
    { codeGeneration: { strings: false, wasm: false } },
  )
  const compiled = new Script(`"use strict";\n(async () => {\n${script}\n})()`)
  const result = compiled.runInContext(context, { timeout: input.timeoutMs })
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      Promise.resolve(result),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("workflow script deadline exceeded")), input.timeoutMs)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value)
}

function makeFileHooks(root: string | undefined) {
  return {
    readFile: async (resource: unknown) => {
      const resolved = resolveWorkspacePath(root, resource)
      if (!(await Bun.file(resolved).exists())) return null
      return Bun.file(resolved).text()
    },
    writeFile: async (resource: unknown, content: unknown) => {
      const resolved = resolveWorkspacePath(root, resource)
      await fs.mkdir(path.dirname(resolved), { recursive: true })
      await Bun.write(resolved, String(content))
    },
    exists: async (resource: unknown) => Bun.file(resolveWorkspacePath(root, resource)).exists(),
    glob: async (pattern: unknown) => {
      const workspace = requireWorkspace(root)
      return (await Array.fromAsync(new Bun.Glob(String(pattern)).scan({ cwd: workspace, dot: true, onlyFiles: false })))
        .map((match) => relativeWorkspacePath(workspace, match))
        .filter((match): match is string => match !== undefined)
        .sort()
    },
  }
}

function requireWorkspace(root: string | undefined) {
  if (!root) throw new Error("Workflow workspace is required for file operations")
  return root
}

function resolveWorkspacePath(root: string | undefined, resource: unknown) {
  const workspace = requireWorkspace(root)
  const resolved = path.resolve(workspace, String(resource))
  const relative = path.relative(workspace, resolved)
  if (relative === "") return resolved
  if (relative.startsWith("..") || path.isAbsolute(relative))
    throw new Error(`Workflow path escapes the workspace: ${String(resource)}`)
  return resolved
}

function relativeWorkspacePath(root: string, resource: string) {
  const resolved = path.resolve(root, resource)
  const relative = path.relative(root, resolved)
  if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) return undefined
  return relative.split(path.sep).join("/")
}

function toSummary(row: typeof WorkflowRunTable.$inferSelect): Summary {
  return {
    runID: row.id,
    sessionID: row.session_id,
    name: row.name,
    status: row.status,
    workspace: row.workspace ?? undefined,
    workspaceManaged: row.workspace_managed,
    workspaceRemoveOnFinish: row.workspace_remove_on_finish,
    workspaceRemoveOnCancel: row.workspace_remove_on_cancel,
    workspaceForceRemove: row.workspace_force_remove,
    scriptSha: row.script_sha,
    running: row.running,
    succeeded: row.succeeded,
    failed: row.failed,
    currentPhase: row.current_phase ?? undefined,
    args: row.args ?? undefined,
    result: row.result ?? undefined,
    error: row.error ?? undefined,
    timeCreated: row.time_created,
    timeUpdated: row.time_updated,
  }
}

function toOutcome(summary: Summary | undefined): RunOutcome | undefined {
  if (!summary) return
  if (summary.status === "completed") return { status: "completed", result: summary.result ?? null }
  if (summary.status === "failed") return { status: "failed", error: summary.error ?? "Workflow failed" }
  if (summary.status === "cancelled") return { status: "cancelled" }
  return
}
