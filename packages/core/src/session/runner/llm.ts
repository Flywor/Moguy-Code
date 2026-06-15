import {
  LLM,
  LLMClient,
  LLMError,
  LLMEvent,
  Message,
  SystemPart,
  isContextOverflowFailure,
  type ProviderErrorEvent,
} from "@opencode-ai/llm"
import { and, asc, eq } from "drizzle-orm"
import { Cause, DateTime, Effect, FiberSet, Layer, Option, Schema, Semaphore, Stream } from "effect"
import path from "node:path"
import { AgentV2 } from "../../agent"
import { Config } from "../../config"
import { Database } from "../../database/database"
import { EventV2 } from "../../event"
import { Location } from "../../location"
import { ModelV2 } from "../../model"
import { ProviderV2 } from "../../provider"
import { QuestionV2 } from "../../question"
import { SystemContext } from "../../system-context/index"
import { SystemContextRegistry } from "../../system-context/registry"
import { SkillGuidance } from "../../skill/guidance"
import { ReferenceGuidance } from "../../reference/guidance"
import { ToolRegistry } from "../../tool/registry"
import { ToolOutputStore } from "../../tool-output-store"
import { Token } from "../../util/token"
import { SessionContextEpoch } from "../context-epoch"
import { SessionCompaction } from "../compaction"
import { SessionEvent } from "../event"
import { SessionHistory } from "../history"
import { SessionInput } from "../input"
import { SessionMaintenance } from "../maintenance"
import { SessionMessage } from "../message"
import { SessionMemorySearch } from "../memory-search"
import { SessionSchema } from "../schema"
import { MemoryFtsTable, SessionTable } from "../sql"
import { SessionStore } from "../store"
import { SessionTask } from "../task"
import { SessionTaskGate } from "../task-gate"
import { type RunError, Service, StepLimitExceededError } from "./index"
import { SessionRunnerModel } from "./model"
import { createLLMEventPublisher } from "./publish-llm-event"
import { toLLMMessages } from "./to-llm-message"

/**
 * Runs one durable coding-agent Session until it settles.
 *
 * Keep this as orchestration over smaller collaborators rather than rebuilding the legacy
 * `SessionPrompt` monolith. Implement the unchecked items in small reviewed slices:
 *
 * - Session ownership and controls
 *   - [x] Coordinate one local active drain per Session; explicit resumes join and prompt wakeups coalesce.
 *   - [ ] Replace local ownership with durable multi-node ownership when clustered.
 *   - [ ] Mark busy, retrying, idle, interrupted, or terminal-failure status durably.
 *   - [ ] Honor interruption and reject stale work after runtime attachment replacement.
 *   - [x] Bound model steps.
 *   - [ ] Bound provider retries and repeated identical tool calls.
 *
 * - Runtime context assembly
 *   - Track V1 runtime-context parity canonically in `specs/v2/session.md`.
 *
 * - One provider turn
 *   - [x] Translate every projected V2 Session message variant into canonical
 *     `@opencode-ai/llm` messages.
 *   - [ ] Resolve policy-filtered built-in, MCP, plugin, and structured-output tool definitions.
 *   - [x] Stream exactly one `llm.stream(request)` provider turn.
 *   - [x] Persist assistant text and usage events incrementally as they arrive.
 *   - [ ] Persist snapshots, patches, and retry notices incrementally as they arrive.
 *   - [x] Persist reasoning, provider errors, and tool-call events incrementally as they arrive.
 *
 * - Tool settlement and continuation
 *   - [x] Durably record each tool call before side effects begin.
 *   - [x] Authorize and execute recorded local calls through a core-owned registry hook.
 *   - [x] Persist typed success, failure, and provider-executed tool outcomes.
 *   - [x] Start each recorded local call eagerly and await all settlements before continuation.
 *   - [ ] Add scoped runtime context, progress updates, attachment normalization,
 *     plugins, and cancellation settlement.
 *   - [x] Reload projected history and start the next explicit provider turn after local tool results.
 *   - [x] Continue for durable user steering accepted during an active provider turn.
 *   - [ ] Continue for compaction or another continuation condition when required.
 *
 * - Post-run maintenance
 *   - [ ] Settle final status and expose durable output events to replayable consumers.
 *   - [ ] Coalesce streamed deltas and add covering projected-history indexes.
 *   - [ ] Update title, summaries, compaction state, and cleanup in bounded background work.
 *
 * Use `llm.stream(request)` for each provider turn. Keep tool execution and continuation here.
 * Durable activity recovery remains a separate future slice with an explicit retry policy.
 *
 * The current slice loads V2 history, translates it, resolves a model through a core service, and persists one
 * provider turn. Registry definitions are advertised, local tool calls are settled durably, and a
 * bounded explicit loop starts the next provider turn after local settlement.
 */

// QUESTION: Did this exist previously, or did we add this limit? Does it make sense?
const MAX_STEPS = 25
const MAX_GOAL_REACT = 12
const DAY_MS = 24 * 60 * 60 * 1_000
const DEFAULT_DREAM_INTERVAL_DAYS = 7
const DEFAULT_DISTILL_INTERVAL_DAYS = 30
const MIN_AUTO_MAINTENANCE_GAP_MS = 10_000
const CHECKPOINT_CONTEXT_TOKEN_BUDGET = 3_500
const PROJECT_MEMORY_TOKEN_BUDGET = 2_500
const TASK_PROGRESS_TOKEN_BUDGET = 1_200
const JUDGE_SYSTEM = `You are evaluating a stop-condition hook. Read the conversation transcript carefully, then judge whether the user-provided condition is satisfied.

Respond only with a JSON object:
{"ok":true,"reason":"quote evidence from the transcript"}
{"ok":false,"reason":"quote what is missing or blocked"}
{"ok":false,"impossible":true,"reason":"explain why the condition can never be satisfied in this session"}

If the transcript does not contain clear evidence that the condition is satisfied, return {"ok":false,"reason":"insufficient evidence in transcript"}. Use impossible only when the condition is genuinely unachievable, not merely incomplete.`

const AUTO_MAINTENANCE_PROMPTS: Record<SessionMaintenance.Kind, string> = {
  dream: [
    "Run one automatic dream memory consolidation pass for the current project.",
    "",
    "Use memory files as the working index and the raw session trajectory as the source of truth.",
    "Consolidate only durable, verified information into project memory.",
    "Keep project memory compact. Do not modify source files except memory assets.",
  ].join("\n"),
  distill: [
    "Run one automatic distill pass for the current project.",
    "",
    "Review recent work and identify repeated manual workflows worth packaging.",
    "Use memory files and concrete session evidence to avoid inventing patterns.",
    "Create only high-confidence missing reusable assets, or report that nothing was worth packaging.",
  ].join("\n"),
}

const AUTO_MAINTENANCE_KINDS = ["dream", "distill"] as const
const lastAutoMaintenance = new Map<string, number>()

type GoalVerdict = {
  readonly ok: boolean
  readonly impossible?: boolean
  readonly reason: string
}

type BudgetedReadResult = {
  readonly text: string
  readonly truncated: boolean
  readonly totalTokens: number
}

type MarkdownSection = {
  readonly header: string
  readonly firstLine: string
  readonly body: readonly string[]
  readonly indexLines: readonly string[]
}

function budgetLines(lines: readonly string[], tokenBudget: number) {
  const result = lines.reduce<{ readonly tokens: number; readonly lines: readonly string[]; readonly truncated: boolean }>(
    (state, line) => {
      if (state.truncated) return state
      const tokens = state.tokens + Token.estimate(line)
      if (tokens > tokenBudget) return { ...state, truncated: true }
      return { tokens, lines: [...state.lines, line], truncated: false }
    },
    { tokens: 0, lines: [], truncated: false },
  )
  return result.truncated ? [...result.lines, "- Additional tasks omitted because the task-progress budget was exhausted."] : result.lines
}

function budgetText(
  filePath: string,
  text: string,
  tokenBudget: number,
  sectionAware = false,
) {
  const totalTokens = Token.estimate(text)
  if (totalTokens <= tokenBudget) return { text, truncated: false, totalTokens }
  if (sectionAware) return readBudgetedSections(filePath, text, tokenBudget, totalTokens)
  const cut = text.slice(0, Math.floor(text.length * (tokenBudget / totalTokens) * 0.95))
  const newline = cut.lastIndexOf("\n")
  const clean = newline > 0 ? cut.slice(0, newline) : cut
  return {
    text: [
      clean,
      "",
      `Truncated at roughly ${tokenBudget} tokens. ${filePath} is roughly ${totalTokens} tokens total; use the read tool for the rest if needed.`,
    ].join("\n"),
    truncated: true,
    totalTokens,
  }
}

function readBudgetedSections(
  filePath: string,
  text: string,
  tokenBudget: number,
  totalTokens: number,
): BudgetedReadResult {
  const parsed = parseMarkdownSections(text)
  const skeleton = [
    ...parsed.preamble,
    ...parsed.sections.flatMap((section) => [section.header, section.firstLine, ...section.indexLines, ""]),
  ]
  const skeletonTokens = Token.estimate(skeleton.join("\n"))
  if (skeletonTokens >= tokenBudget)
    return {
      text: [
        ...skeleton,
        `File is roughly ${totalTokens} tokens and exceeds the ${tokenBudget} token budget. Only the section structure is shown; use the read tool for full content if needed.`,
      ].join("\n"),
      truncated: true,
      totalTokens,
    }

  const lines: string[] = [...parsed.preamble]
  let used = Token.estimate(lines.join("\n"))
  for (const section of parsed.sections) {
    const header = [section.header, section.firstLine, ...section.indexLines].filter(Boolean)
    used += Token.estimate(header.join("\n"))
    lines.push(...header)
    const body = section.body.filter((line) => !section.indexLines.includes(line)).join("\n")
    const bodyTokens = Token.estimate(body)
    if (used + bodyTokens <= tokenBudget) {
      lines.push(body)
      used += bodyTokens
    } else {
      const remaining = tokenBudget - used
      if (remaining > 50) lines.push(body.slice(0, Math.floor(body.length * (remaining / bodyTokens) * 0.95)))
      used = tokenBudget
    }
    lines.push("")
  }
  return {
    text: [
      lines.join("\n"),
      `Truncated at roughly ${tokenBudget} tokens. ${filePath} is roughly ${totalTokens} tokens total; use the read tool for full content if needed.`,
    ].join("\n\n"),
    truncated: true,
    totalTokens,
  }
}

function parseMarkdownSections(text: string) {
  const preamble: string[] = []
  const sections: MarkdownSection[] = []
  let current: { header: string; firstLine: string; body: string[]; indexLines: string[] } | undefined
  for (const line of text.split("\n")) {
    if (line.startsWith("## ")) {
      if (current) sections.push(current)
      current = { header: line, firstLine: "", body: [], indexLines: [] }
      continue
    }
    if (!current) {
      preamble.push(line)
      continue
    }
    if (!current.firstLine && line.startsWith("_") && line.endsWith("_")) {
      current.firstLine = line
      continue
    }
    if (/^- See \S+\.md \(\d+/.test(line.trim())) current.indexLines.push(line)
    current.body.push(line)
  }
  if (current) sections.push(current)
  return { preamble, sections }
}

function renderTaskProgressLine(task: SessionTask.Info, latest: SessionTask.EventInfo | undefined) {
  return [
    `- ${task.id} [${task.status}]`,
    task.parentTaskID ? ` parent=${task.parentTaskID}` : "",
    task.owner ? ` owner=${task.owner}` : "",
    `: ${task.summary}`,
    latest ? `; latest=${latest.kind}${latest.summary ? ` (${latest.summary})` : ""}` : "",
  ].join("")
}

function textOf(message: SessionMessage.Message) {
  if (message.type === "user") return message.text
  if (message.type === "synthetic") return message.text
  if (message.type !== "assistant") return ""
  return message.content
    .flatMap((part) => (part.type === "text" ? [part.text] : []))
    .join("\n")
}

function activeGoalCondition(messages: readonly SessionMessage.Message[]) {
  let condition: string | undefined
  for (const message of messages) {
    const status = message.metadata?.goal_status
    if (status === "cleared" || status === "blocked") condition = undefined
    const objective = message.metadata?.goal_objective
    if (typeof objective === "string" && objective.trim()) condition = objective.trim()
    const text = textOf(message).trim()
    const command = /^\/goal\s+([\s\S]*)$/i.exec(text)
    if (command) {
      const next = command[1].trim()
      condition = ["clear", "reset", "stop"].includes(next.toLowerCase()) ? undefined : next || condition
    }
    const pluginObjective = /^Goal mode is active for this session\.\s+Objective:\s+([\s\S]+?)\s+Start working/im.exec(text)
    if (pluginObjective) condition = pluginObjective[1].trim()
    if (/goal:blocked/i.test(text)) condition = undefined
  }
  return condition
}

function judgeUser(condition: string) {
  return `Based on the conversation transcript above, has the following stopping condition been satisfied? Answer based on transcript evidence only.\n\nCondition: ${condition}`
}

function parseVerdict(text: string): GoalVerdict | undefined {
  const raw = text.trim()
  const json = raw.startsWith("{") ? raw : /\{[\s\S]*\}/.exec(raw)?.[0]
  if (!json) return
  let value: unknown
  try {
    value = JSON.parse(json)
  } catch {
    return
  }
  if (!value || typeof value !== "object") return
  const record = value as Record<string, unknown>
  if (typeof record.ok !== "boolean" || typeof record.reason !== "string") return
  return {
    ok: record.ok,
    impossible: typeof record.impossible === "boolean" ? record.impossible : undefined,
    reason: record.reason,
  }
}

function autoMaintenanceInterval(kind: SessionMaintenance.Kind) {
  return kind === "dream" ? DEFAULT_DREAM_INTERVAL_DAYS : DEFAULT_DISTILL_INTERVAL_DAYS
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const scope = yield* Effect.scope
    const events = yield* EventV2.Service
    const llm = yield* LLMClient.Service
    const agents = yield* AgentV2.Service
    const tools = yield* ToolRegistry.Service
    const models = yield* SessionRunnerModel.Service
    const store = yield* SessionStore.Service
    const location = yield* Location.Service
    const systemContext = yield* SystemContextRegistry.Service
    const skillGuidance = yield* SkillGuidance.Service
    const referenceGuidance = yield* ReferenceGuidance.Service
    const config = yield* Config.Service
    const db = (yield* Database.Service).db
    const compaction = SessionCompaction.make({ events, llm, config: yield* config.entries() })
    const taskRegistry = Option.getOrUndefined(yield* Effect.serviceOption(SessionTask.Service))
    const memorySearch = Option.getOrUndefined(yield* Effect.serviceOption(SessionMemorySearch.Service))
    const maintenance = Option.getOrUndefined(yield* Effect.serviceOption(SessionMaintenance.Service))
    const taskProgressContext = Effect.fn("SessionRunner.taskProgressContext")(function* (
      sessionID: SessionSchema.ID,
    ) {
      if (!taskRegistry) return []
      const tasks = yield* taskRegistry.list({ sessionID, includeTerminal: false }).pipe(Effect.orElseSucceed(() => []))
      if (tasks.length === 0) return []
      const lines = yield* Effect.forEach(
        tasks,
        (task) =>
          taskRegistry
            .events({ sessionID, taskID: task.id })
            .pipe(
              Effect.map((events) => renderTaskProgressLine(task, events.at(-1))),
              Effect.orElseSucceed(() => renderTaskProgressLine(task, undefined)),
            ),
        { concurrency: "unbounded" },
      )
      return [
        "<task-progress>",
        "Durable task registry snapshot for this session. Use it to preserve checkpoint continuity and avoid losing unfinished work.",
        ...budgetLines(lines, TASK_PROGRESS_TOKEN_BUDGET),
        "</task-progress>",
      ]
    })
    const memoryCheckpointContext = Effect.fn("SessionRunner.memoryCheckpointContext")(function* (
      session: SessionSchema.Info,
    ) {
      if (!memorySearch) return []
      const root = yield* memorySearch.root()
      const sections = yield* Effect.forEach(
        [
          {
            tag: "conversation-checkpoint",
            file: path.join(root, "sessions", session.id, "checkpoint.md"),
            budget: CHECKPOINT_CONTEXT_TOKEN_BUDGET,
            description:
              "Latest durable checkpoint for this session. Treat it as historical state and verify details against the live repository when precision matters.",
          },
          {
            tag: "project-memory",
            file: path.join(root, "projects", session.projectID, "MEMORY.md"),
            budget: PROJECT_MEMORY_TOKEN_BUDGET,
            description:
              "Durable project memory from prior sessions. Use it for orientation, rules, and recurring decisions, but prefer current files when they disagree.",
          },
        ] as const,
        (entry) =>
          db
            .select({ body: MemoryFtsTable.body })
            .from(MemoryFtsTable)
            .where(
              and(
                eq(MemoryFtsTable.path, entry.file),
                eq(MemoryFtsTable.scope, entry.tag === "conversation-checkpoint" ? "sessions" : "projects"),
                eq(MemoryFtsTable.scope_id, entry.tag === "conversation-checkpoint" ? session.id : session.projectID),
              ),
            )
            .get()
            .pipe(
              Effect.orDie,
              Effect.map((row) => (row ? budgetText(entry.file, row.body, entry.budget, true) : undefined)),
              Effect.map((result) =>
                result
                  ? [
                      `<${entry.tag} path="${entry.file}">`,
                      entry.description,
                      result.text.trim(),
                      `</${entry.tag}>`,
                    ].join("\n")
                  : undefined,
              ),
            ),
        { concurrency: "unbounded" },
      )
      return sections.filter((section): section is string => section !== undefined)
    })
    const runtimeContext = Effect.fn("SessionRunner.runtimeContext")(function* (session: SessionSchema.Info) {
      const sections = [...(yield* memoryCheckpointContext(session)), ...(yield* taskProgressContext(session.id))]
      if (sections.length === 0) return undefined
      return ["<runtime-context>", ...sections, "</runtime-context>"].join("\n")
    })
    const getSession = Effect.fn("SessionRunner.getSession")(function* (sessionID: SessionSchema.ID) {
      const session = yield* store.get(sessionID)
      if (!session) return yield* Effect.die(`Session not found: ${sessionID}`)
      return session
    })

    const getContext = Effect.fn("SessionRunner.getContext")(function* (sessionID: SessionSchema.ID) {
      return yield* store.context(sessionID)
    })
    const failInterruptedTools = Effect.fn("SessionRunner.failInterruptedTools")(function* (
      sessionID: SessionSchema.ID,
    ) {
      for (const message of yield* getContext(sessionID)) {
        if (message.type !== "assistant") continue
        for (const tool of message.content) {
          if (tool.type !== "tool" || (tool.state.status !== "pending" && tool.state.status !== "running")) continue
          yield* events.publish(SessionEvent.Tool.Failed, {
            sessionID,
            timestamp: yield* DateTime.now,
            assistantMessageID: message.id,
            callID: tool.id,
            error: { type: "unknown", message: "Tool execution interrupted" },
            provider: {
              executed: tool.provider?.executed === true,
              ...(tool.provider?.metadata === undefined ? {} : { metadata: tool.provider.metadata }),
            },
          })
        }
      }
    })

    const awaitToolFibers = (fibers: FiberSet.FiberSet<void, ToolOutputStore.Error>) =>
      Effect.raceFirst(FiberSet.join(fibers), FiberSet.awaitEmpty(fibers))

    // Match V1: dismissing a question halts the loop instead of becoming model-facing tool output.
    const isQuestionRejected = (cause: Cause.Cause<unknown>) =>
      cause.reasons.some((reason) => Cause.isDieReason(reason) && reason.defect instanceof QuestionV2.RejectedError)

    type TurnTransition =
      // Request preparation observed a concurrent Session change and must restart from durable state.
      | { readonly _tag: "RebuildPreparedTurn"; readonly promotion?: SessionInput.Delivery }
      // Overflow compaction completed; rebuild once through the path without overflow recovery.
      | { readonly _tag: "ContinueAfterOverflowCompaction" }

    class TurnTransitionError extends Error {
      constructor(readonly transition: TurnTransition) {
        super()
      }
    }

    const rebuildPreparedTurn = (promotion?: SessionInput.Delivery) =>
      new TurnTransitionError({ _tag: "RebuildPreparedTurn", promotion })
    const continueAfterOverflowCompaction = new TurnTransitionError({
      _tag: "ContinueAfterOverflowCompaction",
    })

    const retryAgentMismatch = (promotion: SessionInput.Delivery | undefined) =>
      Effect.catchDefect((defect) =>
        defect instanceof SessionContextEpoch.AgentMismatch
          ? Effect.die(rebuildPreparedTurn(promotion))
          : Effect.die(defect),
      )

    const sameModel = Schema.toEquivalence(Schema.UndefinedOr(ModelV2.Ref))
    const loadSystemContext = (agent: AgentV2.Selection) =>
      Effect.all([systemContext.load(), skillGuidance.load(agent), referenceGuidance.load()], {
        concurrency: "unbounded",
      }).pipe(Effect.map(SystemContext.combine))

    const runTurnAttempt = Effect.fn("SessionRunner.runTurn")(function* (
      sessionID: SessionSchema.ID,
      promotion: SessionInput.Delivery | undefined,
      recoverOverflow?: typeof compaction.compactAfterOverflow,
    ) {
      const session = yield* getSession(sessionID)
      if (session.location.directory !== location.directory || session.location.workspaceID !== location.workspaceID)
        return yield* Effect.interrupt
      const agent = yield* agents.select(session.agent)
      const initialized = yield* SessionContextEpoch.initialize(
        db,
        loadSystemContext(agent),
        session.id,
        session.location,
        agent.id,
      ).pipe(retryAgentMismatch(promotion))
      const toolFibers = yield* FiberSet.make<void, ToolOutputStore.Error>()
      let needsContinuation = false
      if (promotion) {
        const cutoff = yield* SessionInput.latestSeq(db, session.id)
        if (promotion === "steer") yield* SessionInput.promoteSteers(db, events, session.id, cutoff)
        if (promotion === "queue") {
          yield* SessionInput.promoteNextQueued(db, events, session.id)
          yield* SessionInput.promoteSteers(db, events, session.id, cutoff)
        }
      }
      const system =
        initialized ??
        (yield* SessionContextEpoch.prepare(
          db,
          events,
          loadSystemContext(agent),
          session.id,
          session.location,
          agent.id,
        ).pipe(retryAgentMismatch(undefined)))
      const current = yield* getSession(sessionID)
      if ((yield* agents.select(current.agent)).id !== agent.id || !sameModel(current.model, session.model))
        return yield* Effect.die(rebuildPreparedTurn())
      const model = yield* models.resolve(session)
      const entries = yield* SessionHistory.entriesForRunner(db, session.id, system.baselineSeq)
      const context = entries.map((entry) => entry.message)
      const runtimeContextText = yield* runtimeContext(session)
      const toolMaterialization = yield* tools.materialize(agent.info?.permissions)
      const promptCacheKey = /^ses_[0-9a-f]{64}$/.test(session.id) ? session.id.slice(4) : session.id
      // Keep high-churn history out of the system prefix. DeepSeek's OpenAI-compatible
      // API caches matching prompt prefixes automatically, so the durable baseline must stay
      // byte-stable across tool loops while context updates ride as model-facing messages.
      const request = LLM.request({
        model,
        providerOptions: { openai: { promptCacheKey } },
        system: [agent.info?.system, system.baseline]
          .filter((part): part is string => part !== undefined && part.length > 0)
          .map(SystemPart.make),
        messages: [
          ...toLLMMessages(context, model),
          ...(runtimeContextText === undefined ? [] : [Message.system(runtimeContextText)]),
        ],
        tools: toolMaterialization.definitions,
      })
      if (yield* compaction.compactIfNeeded({ sessionID: session.id, entries, model, request }))
        return yield* Effect.die(rebuildPreparedTurn())
      const publisher = createLLMEventPublisher(events, {
        sessionID: session.id,
        agent: agent.id,
        model: {
          id: ModelV2.ID.make(model.id),
          providerID: ProviderV2.ID.make(model.provider),
          ...(session.model?.variant === undefined ? {} : { variant: session.model.variant }),
        },
      })
      const withPublication = Semaphore.makeUnsafe(1).withPermit
      const publish = (event: LLMEvent, outputPaths: ReadonlyArray<string> = []) =>
        withPublication(publisher.publish(event, outputPaths))
      let overflowFailure: ProviderErrorEvent | undefined
      if (!(yield* SessionContextEpoch.current(db, session.id, agent.id, system.revision)))
        return yield* Effect.die(rebuildPreparedTurn())
      const providerStream = llm.stream(request).pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            if (overflowFailure || publisher.hasProviderError()) return
            if (LLMEvent.is.providerError(event)) {
              if (isContextOverflowFailure(event) && !publisher.hasAssistantStarted()) {
                overflowFailure = event
                return
              }
            }
            yield* publish(event)
            if (event.type !== "tool-call" || event.providerExecuted) return
            needsContinuation = true
            const assistantMessageID = yield* publisher.assistantMessageID(event.id)
            yield* Effect.uninterruptibleMask((restore) =>
              restore(
                toolMaterialization.settle({
                  sessionID: session.id,
                  agent: agent.id,
                  assistantMessageID,
                  call: event,
                }),
              ).pipe(
                Effect.flatMap((settlement) =>
                  publish(
                    LLMEvent.toolResult({
                      id: event.id,
                      name: event.name,
                      result: settlement.result,
                      output: settlement.output,
                    }),
                    settlement.outputPaths ?? [],
                  ),
                ),
              ),
            ).pipe(FiberSet.run(toolFibers))
          }),
        ),
        Effect.ensuring(withPublication(publisher.flush())),
      )

      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const stream = yield* restore(providerStream).pipe(Effect.exit)
          const failure =
            stream._tag === "Failure" ? Option.getOrUndefined(Cause.findErrorOption(stream.cause)) : undefined
          if (
            recoverOverflow &&
            !publisher.hasAssistantStarted() &&
            isContextOverflowFailure(overflowFailure ?? failure) &&
            (yield* restore(recoverOverflow({ sessionID: session.id, entries, model, request })))
          )
            return yield* Effect.die(continueAfterOverflowCompaction)
          if (overflowFailure) yield* publish(overflowFailure)
          const llmFailure = failure instanceof LLMError ? failure : undefined
          if (llmFailure && !publisher.hasProviderError()) {
            yield* withPublication(publisher.failUnsettledTools("Provider did not return a tool result", true))
            yield* withPublication(
              events.publish(SessionEvent.Step.Failed, {
                sessionID: session.id,
                timestamp: yield* DateTime.now,
                assistantMessageID: yield* publisher.startAssistant(),
                error: { type: "unknown", message: llmFailure.reason.message },
              }),
            )
          }
          if (stream._tag === "Failure" && Cause.hasInterrupts(stream.cause)) yield* FiberSet.clear(toolFibers)
          const settled = yield* restore(awaitToolFibers(toolFibers)).pipe(Effect.exit)
          if (settled._tag === "Failure" && isQuestionRejected(settled.cause)) {
            yield* FiberSet.clear(toolFibers)
            yield* withPublication(publisher.failUnsettledTools("Tool execution interrupted"))
            return yield* Effect.interrupt
          }
          if (
            (stream._tag === "Failure" && Cause.hasInterrupts(stream.cause)) ||
            (settled._tag === "Failure" && Cause.hasInterrupts(settled.cause))
          ) {
            yield* FiberSet.clear(toolFibers)
            yield* withPublication(publisher.failUnsettledTools("Tool execution interrupted"))
          }
          if (settled._tag === "Failure" && !Cause.hasInterrupts(settled.cause)) {
            const failure = Cause.squash(settled.cause)
            const message = failure instanceof Error ? failure.message : String(failure)
            yield* withPublication(publisher.failUnsettledTools(`Tool execution failed: ${message}`))
          }
          if (publisher.hasProviderError())
            yield* withPublication(publisher.failUnsettledTools("Tool execution interrupted"))
          if (stream._tag === "Success" && !publisher.hasProviderError())
            yield* withPublication(publisher.failUnsettledTools("Provider did not return a tool result", true))
          if (stream._tag === "Failure") return yield* Effect.failCause(stream.cause)
          if (settled._tag === "Failure") return yield* Effect.failCause(settled.cause)
          return !publisher.hasProviderError() && needsContinuation
        }),
      )
    }, Effect.scoped)
    type RunTurn = (
      sessionID: SessionSchema.ID,
      promotion: SessionInput.Delivery | undefined,
    ) => Effect.Effect<boolean, RunError>

    const runAfterOverflowCompaction: RunTurn = Effect.fnUntraced(function* (sessionID, promotion) {
      return yield* runTurnAttempt(sessionID, promotion).pipe(
        Effect.catchDefect(
          Effect.fnUntraced(function* (defect) {
            if (!(defect instanceof TurnTransitionError)) return yield* Effect.die(defect)
            if (defect.transition._tag === "ContinueAfterOverflowCompaction")
              return yield* Effect.die("Post-compaction provider attempt cannot recover another overflow")
            yield* Effect.yieldNow
            return yield* runAfterOverflowCompaction(sessionID, defect.transition.promotion)
          }),
        ),
      )
    })

    const runTurn: RunTurn = Effect.fnUntraced(function* (sessionID, promotion) {
      return yield* runTurnAttempt(sessionID, promotion, compaction.compactAfterOverflow).pipe(
        Effect.catchDefect(
          Effect.fnUntraced(function* (defect) {
            if (!(defect instanceof TurnTransitionError)) return yield* Effect.die(defect)
            yield* Effect.yieldNow
            if (defect.transition._tag === "ContinueAfterOverflowCompaction")
              return yield* runAfterOverflowCompaction(sessionID, undefined)
            return yield* runTurn(sessionID, defect.transition.promotion)
          }),
        ),
      )
    })

    const injectReminder = Effect.fn("SessionRunner.injectReminder")(function* (sessionID: SessionSchema.ID, text: string) {
      yield* events.publish(SessionEvent.Synthetic, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: yield* DateTime.now,
        text,
      })
    })

    const taskGateReentry = Effect.fn("SessionRunner.taskGateReentry")(function* (
      sessionID: SessionSchema.ID,
      reactCount: number,
    ) {
      if (!taskRegistry) return false
      const decision = yield* SessionTaskGate.decide({ tasks: taskRegistry, sessionID, reactCount })
      if (decision.needReentry) {
        yield* injectReminder(sessionID, decision.reentryText)
        return true
      }
      if (decision.capExceeded)
        yield* Effect.logWarning("task gate hit cap; allowing stop", { sessionID, tasks: decision.incompleteTasks })
      return false
    })

    const goalJudgeReentry = Effect.fn("SessionRunner.goalJudgeReentry")(function* (
      sessionID: SessionSchema.ID,
      reactCount: number,
    ) {
      if (reactCount >= MAX_GOAL_REACT) {
        yield* Effect.logWarning("goal judge hit cap; allowing stop", { sessionID })
        return false
      }
      const context = yield* getContext(sessionID)
      const condition = activeGoalCondition(context)
      if (!condition) return false
      const session = yield* getSession(sessionID)
      const model = yield* models.resolve(session)
      const chunks: string[] = []
      let failed = false
      const judged = yield* llm
        .stream(
          LLM.request({
            model,
            messages: [Message.system(JUDGE_SYSTEM), ...toLLMMessages(context, model), Message.user(judgeUser(condition))],
            tools: [],
          }),
        )
        .pipe(
          Stream.runForEach((event) => {
            if (LLMEvent.is.providerError(event)) failed = true
            if (LLMEvent.is.textDelta(event)) chunks.push(event.text)
            return Effect.void
          }),
          Effect.as(true),
          Effect.catchTag("LLM.Error", () => Effect.succeed(false)),
        )
      const verdict = judged && !failed ? parseVerdict(chunks.join("")) : undefined
      if (!verdict) {
        yield* Effect.logWarning("goal judge failed; allowing stop", { sessionID })
        return false
      }
      if (verdict.ok || verdict.impossible) return false
      yield* injectReminder(
        sessionID,
        [
          "<system-reminder>",
          "An independent goal judge says the active goal is not satisfied yet.",
          `<goal>${condition}</goal>`,
          `<judge-reason>${verdict.reason}</judge-reason>`,
          "Keep working toward the goal. Do not stop until it is genuinely met or impossible.",
          "</system-reminder>",
        ].join("\n"),
      )
      return true
    })

    const shouldRunAutoMaintenance = Effect.fn("SessionRunner.shouldRunAutoMaintenance")(function* (
      session: SessionSchema.Info,
      kind: SessionMaintenance.Kind,
    ) {
      if (!maintenance || !memorySearch) return false
      const now = Date.now()
      const key = `${session.projectID}:${kind}`
      if (now - (lastAutoMaintenance.get(key) ?? 0) < MIN_AUTO_MAINTENANCE_GAP_MS) return false
      const intervalMs = autoMaintenanceInterval(kind) * DAY_MS
      const root = yield* memorySearch.root()
      const file = path.join(root, "projects", session.projectID, "auto", `${kind}.md`)
      const ledger = yield* db
        .select({ body: MemoryFtsTable.body })
        .from(MemoryFtsTable)
        .where(eq(MemoryFtsTable.path, file))
        .get()
        .pipe(Effect.orDie)
      const lastRun = Number(/^last_run:\s*(\d+)$/m.exec(ledger?.body ?? "")?.[1])
      if (Number.isFinite(lastRun) && now - lastRun < intervalMs) return false
      if (!Number.isFinite(lastRun)) {
        const earliest = yield* db
          .select({ timeCreated: SessionTable.time_created })
          .from(SessionTable)
          .where(eq(SessionTable.project_id, session.projectID))
          .orderBy(asc(SessionTable.time_created))
          .get()
          .pipe(Effect.orDie)
        if (!earliest || now - earliest.timeCreated < intervalMs) return false
      }
      lastAutoMaintenance.set(key, now)
      yield* memorySearch.write({
        scope: "projects",
        scopeID: session.projectID,
        key: `auto/${kind}`,
        body: [
          `# Auto ${kind}`,
          "",
          `last_run: ${now}`,
          `updated: ${new Date(now).toISOString()}`,
          "",
          "This file records automatic maintenance scheduling for this project.",
        ].join("\n"),
      })
      return true
    })

    const requestAutoMaintenance = Effect.fn("SessionRunner.requestAutoMaintenance")(function* (
      sessionID: SessionSchema.ID,
    ) {
      if (!maintenance) return
      const session = yield* getSession(sessionID)
      for (const kind of AUTO_MAINTENANCE_KINDS) {
        if (!(yield* shouldRunAutoMaintenance(session, kind))) continue
        yield* maintenance
          .request({ session, kind, prompt: AUTO_MAINTENANCE_PROMPTS[kind] })
          .pipe(
            Effect.catchCause((cause) => Effect.logWarning("auto maintenance failed", { sessionID, kind, cause })),
            Effect.forkIn(scope, { startImmediately: true }),
          )
      }
    })

    const run = Effect.fn("SessionRunner.run")(function* (input: {
      readonly sessionID: SessionSchema.ID
      readonly force?: boolean
    }) {
      const hasSteer = yield* SessionInput.hasPending(db, input.sessionID, "steer")
      const hasQueue = hasSteer ? false : yield* SessionInput.hasPending(db, input.sessionID, "queue")
      if (input.force !== true && !hasSteer && !hasQueue) return
      yield* failInterruptedTools(input.sessionID)
      let promotion: SessionInput.Delivery | undefined = hasSteer ? "steer" : hasQueue ? "queue" : undefined
      let openActivity = input.force === true || hasSteer || hasQueue
      while (openActivity) {
        let needsContinuation = true
        let taskGateReact = 0
        let goalJudgeReact = 0
        for (let step = 0; step < MAX_STEPS; step++) {
          needsContinuation = yield* runTurn(input.sessionID, promotion)
          promotion = "steer"
          if (!needsContinuation) needsContinuation = yield* SessionInput.hasPending(db, input.sessionID, "steer")
          if (!needsContinuation && (yield* taskGateReentry(input.sessionID, taskGateReact))) {
            taskGateReact++
            needsContinuation = true
          }
          if (!needsContinuation && (yield* goalJudgeReentry(input.sessionID, goalJudgeReact))) {
            goalJudgeReact++
            needsContinuation = true
          }
          if (!needsContinuation) break
        }
        if (needsContinuation)
          return yield* new StepLimitExceededError({ sessionID: input.sessionID, limit: MAX_STEPS })
        openActivity = yield* SessionInput.hasPending(db, input.sessionID, "queue")
        promotion = openActivity ? "queue" : undefined
      }
      yield* requestAutoMaintenance(input.sessionID)
    })

    return Service.of({
      run,
    })
  }),
)

export const defaultLayer = layer
