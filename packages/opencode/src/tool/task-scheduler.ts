import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceState } from "@/effect/instance-state"
import type { SessionID } from "@/session/schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Context, Effect, Layer, Option, Schema, Semaphore } from "effect"

export type TaskKind = "read" | "write" | "test" | "review" | "plan"
export type TaskIsolation = "readonly" | "ownership" | "patch" | "worktree"

export type TaskScope = {
  files?: readonly string[]
  symbols?: readonly string[]
  topics?: readonly string[]
  operations?: readonly TaskKind[]
}

export type TaskModelBudget = {
  maxTokens?: number
  maxCost?: number
}

export type ScheduleParams = {
  description: string
  prompt: string
  subagent_type: string
  task_id?: string
  scope?: TaskScope
  expected_output?: string
  timeout_ms?: number
  model_budget?: TaskModelBudget
  depends_on?: readonly string[]
  task_kind?: TaskKind
  owned_files?: readonly string[]
  merge_key?: string
  isolation?: TaskIsolation
}

export type PreparedTask =
  | {
      type: "new"
      record: TaskRecord
      prompt: string
    }
  | {
      type: "duplicate"
      record: TaskRecord
      prompt: string
    }

export type TaskRecord = {
  id: string
  parentSessionID: SessionID
  sessionID?: SessionID
  description: string
  mergeKey: string
  kind: TaskKind
  isolation: TaskIsolation
  scope: TaskScope
  expectedOutput?: string
  timeoutMS?: number
  modelBudget?: TaskModelBudget
  dependsOn: string[]
  ownedFiles: string[]
  status: "pending" | "running" | "completed" | "error" | "cancelled"
  output?: string
  updatedAt: number
}

type BlackboardFact = {
  taskID: string
  sessionID?: SessionID
  type: "file" | "symbol" | "conclusion" | "risk" | "recommendation" | "unresolved"
  text: string
  confidence?: number
  evidence?: string[]
}

type Conflict = {
  taskID: string
  againstTaskID?: string
  text: string
}

type ParentState = {
  brief: string
  tasks: Map<string, TaskRecord>
  byMergeKey: Map<string, string>
  locks: Map<string, string>
  facts: BlackboardFact[]
  conflicts: Conflict[]
  next: number
}

type State = {
  parents: Map<SessionID, ParentState>
  semaphores: Record<TaskKind, Semaphore.Semaphore>
}

type StructuredOutput = {
  summary?: string
  files: string[]
  symbols: string[]
  conclusions: string[]
  risks: string[]
  recommendations: string[]
  unresolved: string[]
  conflicts: string[]
  confidence?: number
  evidence: string[]
}

const decodeJson = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)

export interface Interface {
  readonly prepare: (input: {
    parentSessionID: SessionID
    params: ScheduleParams
    messages: SessionV1.WithParts[]
  }) => Effect.Effect<PreparedTask, Error>
  readonly assignSession: (input: { recordID: string; sessionID: SessionID }) => Effect.Effect<TaskRecord, Error>
  readonly run: <A, E, R>(recordID: string, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | Error, R>
  readonly complete: (input: {
    recordID: string
    output: string
  }) => Effect.Effect<{ output: string; reviewPrompt?: string }, Error>
  readonly fail: (input: {
    recordID: string
    status: "error" | "cancelled"
    error?: string
  }) => Effect.Effect<void, Error>
  readonly metadata: (record: TaskRecord) => Record<string, unknown>
  readonly readonlyPermissionRules: (
    record: TaskRecord,
  ) => Array<{ permission: "edit"; pattern: string; action: "deny" }>
  readonly ownershipPermissionRules: (
    record: TaskRecord,
  ) => Array<{ permission: "edit"; pattern: string; action: "allow" | "deny" }>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/TaskScheduler") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const state = yield* InstanceState.make<State>(() =>
      Effect.succeed({
        parents: new Map(),
        semaphores: {
          read: Semaphore.makeUnsafe(8),
          write: Semaphore.makeUnsafe(1),
          test: Semaphore.makeUnsafe(1),
          review: Semaphore.makeUnsafe(2),
          plan: Semaphore.makeUnsafe(3),
        },
      }),
    )

    const getParent = Effect.fn("TaskScheduler.getParent")(function* (
      parentSessionID: SessionID,
      messages: SessionV1.WithParts[],
      fallback: string,
    ) {
      const current = yield* InstanceState.get(state)
      const existing = current.parents.get(parentSessionID)
      if (existing) return existing
      const parent: ParentState = {
        brief: buildBrief(messages, fallback),
        tasks: new Map(),
        byMergeKey: new Map(),
        locks: new Map(),
        facts: [],
        conflicts: [],
        next: 0,
      }
      current.parents.set(parentSessionID, parent)
      return parent
    })

    const findRecord = Effect.fn("TaskScheduler.findRecord")(function* (recordID: string) {
      const current = yield* InstanceState.get(state)
      for (const parent of current.parents.values()) {
        const record = parent.tasks.get(recordID)
        if (record) return { parent, record }
      }
      return yield* Effect.fail(new Error(`Unknown scheduled task: ${recordID}`))
    })

    const prepare: Interface["prepare"] = Effect.fn("TaskScheduler.prepare")(function* (input) {
      const parent = yield* getParent(input.parentSessionID, input.messages, input.params.description)
      const kind = input.params.task_kind ?? inferKind(input.params)
      const scope = normalizeScope(input.params.scope)
      const ownedFiles = unique([...(input.params.owned_files ?? []), ...(kind === "write" ? (scope.files ?? []) : [])])
      if (kind === "write" && ownedFiles.length === 0) {
        return yield* Effect.fail(
          new Error("Writing subagent tasks require owned_files or scope.files so the scheduler can isolate writes"),
        )
      }

      const mergeKey = normalizeMergeKey(input.params, kind, scope)
      const duplicate = input.params.task_id ? undefined : parent.tasks.get(parent.byMergeKey.get(mergeKey) ?? "")
      if (duplicate?.sessionID && ["running", "completed"].includes(duplicate.status)) {
        return {
          type: "duplicate" as const,
          record: duplicate,
          prompt: renderScheduledPrompt(parent, duplicate, input.params.prompt),
        }
      }

      const conflict = ownedFiles
        .map((file) => ({ file, owner: parent.locks.get(file) }))
        .find((item) => item.owner && parent.tasks.get(item.owner)?.status === "running")
      if (conflict?.owner) {
        const owner = parent.tasks.get(conflict.owner)
        return yield* Effect.fail(
          new Error(
            `File ownership conflict: ${conflict.file} is already locked by task ${owner?.sessionID ?? owner?.id}`,
          ),
        )
      }

      const record: TaskRecord = {
        id: `task-${Date.now().toString(36)}-${parent.next++}`,
        parentSessionID: input.parentSessionID,
        description: input.params.description,
        mergeKey,
        kind,
        isolation: input.params.isolation ?? defaultIsolation(kind),
        scope,
        expectedOutput: input.params.expected_output,
        timeoutMS: input.params.timeout_ms,
        modelBudget: input.params.model_budget,
        dependsOn: [...(input.params.depends_on ?? [])],
        ownedFiles,
        status: "pending",
        updatedAt: Date.now(),
      }
      parent.tasks.set(record.id, record)
      parent.byMergeKey.set(mergeKey, record.id)
      for (const file of ownedFiles) {
        parent.locks.set(file, record.id)
      }
      return { type: "new" as const, record, prompt: renderScheduledPrompt(parent, record, input.params.prompt) }
    })

    const assignSession: Interface["assignSession"] = Effect.fn("TaskScheduler.assignSession")(function* (input) {
      const { record } = yield* findRecord(input.recordID)
      record.sessionID = input.sessionID
      record.status = "running"
      record.updatedAt = Date.now()
      return record
    })

    const run: Interface["run"] = (recordID, effect) =>
      Effect.gen(function* () {
        const { record } = yield* findRecord(recordID)
        const current = yield* InstanceState.get(state)
        const task = current.semaphores[record.kind].withPermit(effect)
        if (!record.timeoutMS) return yield* task
        return yield* task.pipe(
          Effect.timeoutOrElse({
            duration: `${record.timeoutMS} millis`,
            orElse: () => Effect.fail(new Error(`Task timed out after ${record.timeoutMS}ms`)),
          }),
        )
      })

    const complete: Interface["complete"] = Effect.fn("TaskScheduler.complete")(function* (input) {
      const { parent, record } = yield* findRecord(input.recordID)
      record.status = "completed"
      record.output = input.output
      record.updatedAt = Date.now()
      releaseLocks(parent, record)
      const structured = parseStructuredOutput(input.output)
      const conflicts = structured ? writeStructuredOutput(parent, record, structured) : []
      const report = renderFanInReport(parent, record, structured, conflicts)
      return {
        output: [input.output, report].filter(Boolean).join("\n\n"),
        ...(conflicts.length ? { reviewPrompt: renderReviewPrompt(parent, conflicts) } : {}),
      }
    })

    const fail: Interface["fail"] = Effect.fn("TaskScheduler.fail")(function* (input) {
      const { parent, record } = yield* findRecord(input.recordID)
      record.status = input.status
      record.output = input.error
      record.updatedAt = Date.now()
      releaseLocks(parent, record)
    })

    const metadata: Interface["metadata"] = (record) => ({
      scheduler: {
        taskId: record.id,
        kind: record.kind,
        isolation: record.isolation,
        scope: record.scope,
        expectedOutput: record.expectedOutput,
        timeoutMs: record.timeoutMS,
        modelBudget: record.modelBudget,
        dependsOn: record.dependsOn,
        ownedFiles: record.ownedFiles,
        mergeKey: record.mergeKey,
      },
    })

    const readonlyPermissionRules: Interface["readonlyPermissionRules"] = (record) =>
      record.kind === "read" || record.kind === "review" || record.kind === "plan" || record.isolation === "readonly"
        ? [{ permission: "edit", pattern: "*", action: "deny" }]
        : []

    const ownershipPermissionRules: Interface["ownershipPermissionRules"] = (record) => {
      if (record.kind !== "write") return []
      if (record.isolation === "patch" || record.isolation === "worktree") {
        return [{ permission: "edit", pattern: "*", action: "deny" }]
      }
      return [
        { permission: "edit", pattern: "*", action: "deny" },
        ...record.ownedFiles.map((file) => ({ permission: "edit" as const, pattern: file, action: "allow" as const })),
      ]
    }

    return Service.of({
      prepare,
      assignSession,
      run,
      complete,
      fail,
      metadata,
      readonlyPermissionRules,
      ownershipPermissionRules,
    })
  }),
)

export const defaultLayer = layer

export const node = LayerNode.make(layer, [])

function buildBrief(messages: SessionV1.WithParts[], fallback: string) {
  const text = messages
    .filter((message) => message.info.role === "user")
    .flatMap((message) => message.parts)
    .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text.trim()] : []))
    .filter(Boolean)
    .slice(-3)
    .join("\n\n")
  return truncate(text || fallback, 1_500)
}

function normalizeScope(scope?: TaskScope): TaskScope {
  return {
    ...(scope?.files?.length ? { files: unique(scope.files) } : {}),
    ...(scope?.symbols?.length ? { symbols: unique(scope.symbols) } : {}),
    ...(scope?.topics?.length ? { topics: unique(scope.topics) } : {}),
    ...(scope?.operations?.length ? { operations: unique(scope.operations) } : {}),
  }
}

function inferKind(params: ScheduleParams): TaskKind {
  const text = `${params.description}\n${params.prompt}`.toLowerCase()
  if (params.subagent_type === "explore") return "read"
  if (text.match(/\b(review|audit|inspect diff|check diff)\b/)) return "review"
  if (text.match(/\b(test|typecheck|lint|verify|ci)\b/)) return "test"
  if (params.owned_files?.length || text.match(/\b(write|edit|modify|implement|fix|refactor|apply patch)\b/))
    return "write"
  return "read"
}

function defaultIsolation(kind: TaskKind): TaskIsolation {
  if (kind === "write") return "ownership"
  if (kind === "test") return "readonly"
  if (kind === "review") return "readonly"
  return "readonly"
}

function normalizeMergeKey(params: ScheduleParams, kind: TaskKind, scope: TaskScope) {
  if (params.merge_key) return params.merge_key
  return [
    params.subagent_type,
    kind,
    JSON.stringify(scope),
    normalizeText(params.prompt || params.description).slice(0, 500),
  ].join("|")
}

function renderScheduledPrompt(parent: ParentState, record: TaskRecord, prompt: string) {
  return [
    "<task_brief>",
    parent.brief,
    "</task_brief>",
    renderBlackboard(parent, record.kind),
    "<scheduler_contract>",
    `task_id: ${record.id}`,
    `kind: ${record.kind}`,
    `scope: ${JSON.stringify(record.scope)}`,
    record.expectedOutput ? `expected_output: ${record.expectedOutput}` : undefined,
    record.timeoutMS ? `timeout_ms: ${record.timeoutMS}` : undefined,
    record.modelBudget ? `model_budget: ${JSON.stringify(record.modelBudget)}` : undefined,
    record.dependsOn.length ? `depends_on: ${record.dependsOn.join(", ")}` : undefined,
    record.ownedFiles.length ? `owned_files: ${record.ownedFiles.join(", ")}` : undefined,
    `isolation: ${record.isolation}`,
    record.isolation === "patch"
      ? "Patch isolation: do not edit files directly. Return a unified patch/diff for the main agent to merge."
      : undefined,
    record.isolation === "worktree"
      ? "Worktree isolation requested: do not edit the parent worktree directly. Return a patch/diff unless an isolated worktree is attached by the host."
      : undefined,
    "Return a concise final answer plus a fenced ```json block with keys: summary, files, symbols, conclusions, risks, recommendations, unresolved, confidence, evidence.",
    "</scheduler_contract>",
    "<task_prompt>",
    prompt,
    "</task_prompt>",
  ]
    .filter((line): line is string => typeof line === "string" && line.length > 0)
    .join("\n")
}

const allowedFactTypes: Record<TaskKind, ReadonlySet<BlackboardFact["type"]>> = {
  read: new Set(["file", "symbol", "unresolved"]),
  write: new Set(["file", "symbol", "conclusion", "risk", "unresolved"]),
  review: new Set(["file", "symbol", "conclusion", "unresolved"]),
  test: new Set(["file", "symbol", "conclusion", "risk", "unresolved"]),
  plan: new Set(["file", "symbol", "conclusion", "risk", "recommendation", "unresolved"]),
}

function renderBlackboard(parent: ParentState, kind: TaskKind) {
  if (parent.facts.length === 0 && parent.conflicts.length === 0)
    return "<shared_blackboard>No prior facts.</shared_blackboard>"
  const allowed = allowedFactTypes[kind]
  const facts = parent.facts
    .filter((fact) => allowed.has(fact.type))
    .slice(-40)
    .map((fact) => `- [${fact.type}] ${fact.text}${fact.confidence === undefined ? "" : ` (${fact.confidence})`}`)
  const conflicts = parent.conflicts.slice(-10).map((conflict) => `- ${conflict.text}`)
  return [
    "<shared_blackboard>",
    facts.length ? "Known facts:" : undefined,
    ...facts,
    conflicts.length ? "Open conflicts:" : undefined,
    ...conflicts,
    "</shared_blackboard>",
  ]
    .filter((line): line is string => typeof line === "string")
    .join("\n")
}

function parseStructuredOutput(text: string): StructuredOutput | undefined {
  const candidates = [text.match(/```json\s*([\s\S]*?)```/i)?.[1], text.trim()].filter(
    (item): item is string => typeof item === "string" && item.trim().length > 0,
  )
  for (const candidate of candidates) {
    const option = decodeJson(candidate.trim())
    if (Option.isNone(option) || !isRecord(option.value)) continue
    return normalizeStructuredOutput(option.value)
  }
  return undefined
}

function normalizeStructuredOutput(record: Record<string, unknown>): StructuredOutput {
  return {
    summary: stringValue(record.summary),
    files: stringList(record.files),
    symbols: stringList(record.symbols),
    conclusions: stringList(record.conclusions),
    risks: stringList(record.risks),
    recommendations: stringList(record.recommendations),
    unresolved: stringList(record.unresolved),
    conflicts: stringList(record.conflicts),
    confidence: numberValue(record.confidence),
    evidence: evidenceList(record.evidence),
  }
}

function writeStructuredOutput(parent: ParentState, record: TaskRecord, structured: StructuredOutput) {
  const additions: BlackboardFact[] = [
    ...structured.files.map((text) => fact(record, "file", text, structured)),
    ...structured.symbols.map((text) => fact(record, "symbol", text, structured)),
    ...structured.conclusions.map((text) => fact(record, "conclusion", text, structured)),
    ...structured.risks.map((text) => fact(record, "risk", text, structured)),
    ...structured.recommendations.map((text) => fact(record, "recommendation", text, structured)),
    ...structured.unresolved.map((text) => fact(record, "unresolved", text, structured)),
  ]
  const conflicts = [
    ...structured.conflicts.map((text) => ({ taskID: record.id, text })),
    ...structured.conclusions.flatMap((text) =>
      parent.facts
        .filter((item) => item.type === "conclusion" && isContradiction(text, item.text))
        .map((item) => ({
          taskID: record.id,
          againstTaskID: item.taskID,
          text: `Conflicting conclusions: "${text}" vs "${item.text}"`,
        })),
    ),
  ]
  parent.facts.push(...additions)
  parent.conflicts.push(...conflicts)
  return conflicts
}

function fact(record: TaskRecord, type: BlackboardFact["type"], text: string, structured: StructuredOutput) {
  return {
    taskID: record.id,
    sessionID: record.sessionID,
    type,
    text,
    confidence: structured.confidence,
    evidence: structured.evidence,
  }
}

function renderFanInReport(
  parent: ParentState,
  record: TaskRecord,
  structured: StructuredOutput | undefined,
  conflicts: Conflict[],
) {
  const facts = parent.facts.filter((fact) => fact.taskID === record.id).slice(0, 12)
  if (!structured && facts.length === 0 && conflicts.length === 0) return ""
  return [
    "<task_fan_in>",
    structured?.summary ? `summary: ${structured.summary}` : undefined,
    facts.length ? "new_findings:" : undefined,
    ...facts.map((fact) => `- [${fact.type}] ${fact.text}`),
    conflicts.length ? "conflicts:" : undefined,
    ...conflicts.map((conflict) => `- ${conflict.text}`),
    `completed_tasks: ${Array.from(parent.tasks.values()).filter((task) => task.status === "completed").length}`,
    "</task_fan_in>",
  ]
    .filter((line): line is string => typeof line === "string" && line.length > 0)
    .join("\n")
}

function renderReviewPrompt(parent: ParentState, conflicts: Conflict[]) {
  return [
    "Review the conflicting subagent findings below. Use only the shared facts and cite the specific files or symbols when possible.",
    "",
    "<task_brief>",
    parent.brief,
    "</task_brief>",
    "",
    "<conflicts>",
    ...conflicts.map((conflict) => `- ${conflict.text}`),
    "</conflicts>",
    "",
    "Return a short verdict and a fenced ```json block with summary, conclusions, risks, recommendations, unresolved, confidence, and evidence.",
  ].join("\n")
}

function releaseLocks(parent: ParentState, record: TaskRecord) {
  for (const file of record.ownedFiles) {
    if (parent.locks.get(file) === record.id) parent.locks.delete(file)
  }
}

function isContradiction(a: string, b: string) {
  const left = normalizeText(a)
  const right = normalizeText(b)
  if (!left || !right || left === right) return false
  return stripNegation(left) === stripNegation(right) && hasNegation(left) !== hasNegation(right)
}

function stripNegation(input: string) {
  return input
    .replace(/\b(is|are|was|were|does|do|did|can|cannot|should|must)\b/g, "")
    .replace(/\b(not|no|never|none|without|cannot|can't|doesn't|don't|didn't|isn't|aren't)\b/g, "")
    .replace(/\s+/g, " ")
    .trim()
}

function hasNegation(input: string) {
  return /\b(not|no|never|none|without|cannot|can't|doesn't|don't|didn't|isn't|aren't)\b/.test(input)
}

function normalizeText(input: string) {
  return input
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s/._-]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim()
}

function unique<T>(items: readonly T[]) {
  return Array.from(new Set(items.filter((item) => item !== undefined && item !== null)))
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function stringValue(value: unknown) {
  return typeof value === "string" && value.trim() ? value.trim() : undefined
}

function numberValue(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined
}

function stringList(value: unknown) {
  if (typeof value === "string" && value.trim()) return [value.trim()]
  if (!Array.isArray(value)) return []
  return value.flatMap((item) => {
    if (typeof item === "string" && item.trim()) return [item.trim()]
    if (isRecord(item))
      return [stringValue(item.text) ?? stringValue(item.summary) ?? stringValue(item.file)].filter(
        (text): text is string => typeof text === "string",
      )
    return []
  })
}

function evidenceList(value: unknown) {
  if (typeof value === "string" && value.trim()) return [value.trim()]
  if (!Array.isArray(value)) return []
  return value.flatMap((item) => {
    if (typeof item === "string" && item.trim()) return [item.trim()]
    if (!isRecord(item)) return []
    return [
      [stringValue(item.file), stringValue(item.symbol), numberValue(item.line)?.toString(), stringValue(item.detail)]
        .filter((part): part is string => typeof part === "string" && part.length > 0)
        .join(":"),
    ].filter(Boolean)
  })
}

function truncate(input: string, length: number) {
  if (input.length <= length) return input
  return input.slice(0, length).trimEnd() + "\n..."
}

export const TaskScheduler = {
  Service,
  layer,
  defaultLayer,
  node,
}
