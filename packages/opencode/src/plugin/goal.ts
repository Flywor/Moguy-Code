import type { Event, Part } from "@opencode-ai/sdk"
import type { Hooks, PluginInput } from "@opencode-ai/plugin"

const GOAL_COMMAND = "goal"
const GOAL_AGENT = "build"
const MAX_CONTINUATIONS = 50
const CONTINUE_DELAY_MS = 1_000
const META_COMMAND = "goal_command"
const META_OBJECTIVE = "goal_objective"
const META_STATUS = "goal_status"
const META_CONTINUE = "goal_continue"
const COMPLETION_EVIDENCE_SECTIONS = [
  /verification loop[:：]/i,
  /environment validation[:：]/i,
  /(boundary|fuzz)(\/boundary)? checks?[:：]/i,
  /independent evaluator[:：]/i,
]

type GoalState = {
  objective: string
  continuations: number
  pending: boolean
  status: "active" | "complete" | "blocked"
}
type TextCommandPart = Extract<Part, { type: "text" }>
type MessageWithParts = {
  info: {
    id: string
    sessionID: string
    role: "user" | "assistant"
    error?: unknown
  }
  parts: Part[]
}
type GoalMarker =
  | {
      messageIndex: number
      kind: "clear"
    }
  | {
      messageIndex: number
      kind: "start"
      objective: string
    }

export async function GoalPlugin(input: PluginInput): Promise<Hooks> {
  const goals = new Map<string, GoalState>()

  return {
    "command.execute.before": async (command, output) => {
      if (command.command !== GOAL_COMMAND) return

      const objective = command.arguments.trim()
      if (["clear", "reset", "stop"].includes(objective.toLowerCase())) {
        goals.delete(command.sessionID)
        setText(output.parts, "Goal cleared for this session.", {
          [META_COMMAND]: "clear",
          [META_STATUS]: "cleared",
        })
        return
      }

      if (objective.toLowerCase() === "status") {
        setText(output.parts, goalStatus(await loadGoal(input, goals, command.sessionID)))
        return
      }

      if (!objective) {
        setText(output.parts, "Usage: /goal <objective>. Example: /goal finish the failing typecheck.")
        return
      }

      goals.set(command.sessionID, {
        objective,
        continuations: 0,
        pending: false,
        status: "active",
      })
      setText(
        output.parts,
        [
          "Goal mode is active for this session.",
          "",
          `Objective: ${objective}`,
          "",
          "Start working toward the objective now. Continue until it is complete or blocked.",
        ].join("\n"),
        {
          [META_COMMAND]: "start",
          [META_OBJECTIVE]: objective,
          [META_STATUS]: "active",
        },
      )
    },
    "experimental.chat.system.transform": async (chat, output) => {
      if (!chat.sessionID) return
      const goal = await loadGoal(input, goals, chat.sessionID)
      if (!goal || goal.status !== "active") return
      output.system.push(goalSystem(goal))
    },
    event: async (event) => {
      markFromEvent(goals, event.event)
      if (event.event.type !== "session.idle") return
      await continueGoal(input, goals, event.event.properties.sessionID)
    },
  }
}

function setText(parts: Part[], text: string, metadata?: Record<string, unknown>) {
  const first = parts.find((part): part is TextCommandPart => part.type === "text")
  if (first) {
    first.text = text
    if (metadata) first.metadata = { ...(first.metadata ?? {}), ...metadata }
    return
  }
  parts.splice(0, parts.length, { type: "text", text, metadata } as TextCommandPart)
}

function goalStatus(goal: GoalState | undefined) {
  if (!goal) return "No active goal is set for this session."
  return [
    `Goal status: ${goal.status}.`,
    `Objective: ${goal.objective}`,
    `Automatic continuations used: ${goal.continuations}/${MAX_CONTINUATIONS}.`,
  ].join("\n")
}

function goalSystem(goal: GoalState) {
  return [
    "## Active Session Goal",
    "",
    "The user has set a persistent session goal. Own the outcome end-to-end until the goal is complete or genuinely blocked.",
    "",
    "<goal_objective>",
    goal.objective,
    "</goal_objective>",
    "",
    `Automatic continuations used: ${goal.continuations}/${MAX_CONTINUATIONS}.`,
    "",
    "Mandatory loop:",
    "1. Start each goal turn with a concise status check: objective, completion criteria, current evidence, blocker status, and next experiment.",
    "2. Use a scientific experiment loop: hypothesis -> action/experiment -> observation -> decision -> next step.",
    "3. Keep an experiment log in the conversation. Each entry must record what was tested or changed, the observed result, and the decision it supports.",
    "4. For code-producing work, run a verification loop before completion: generate/change code -> run focused tests -> run install/package verification -> run end-to-end or smoke verification -> report gaps -> fix -> repeat until the evidence is clean.",
    "5. Before final acceptance, validate in an isolated real environment when feasible: Docker, a fresh virtualenv, a fresh temp workspace, or the closest project-native equivalent. If this cannot run, record the exact blocker and residual risk.",
    "6. Fuzz the produced behavior beyond the happy path when the artifact accepts input or has observable runtime behavior: empty input, very long input, special characters, concurrent use, offline/network failure, and insufficient permissions where relevant.",
    "7. Run an independent evaluator pass before completion when the Task tool and an evaluator agent are available. Prompt it to find problems, attack assumptions, and report missing verification rather than to confirm correctness.",
    "8. Continue autonomously when the next step is clear. Ask the user only when progress is unsafe or impossible without missing input.",
    "",
    "Strict completion protocol:",
    "- Do not claim completion until every completion criterion is satisfied by concrete evidence.",
    "- A valid completion response must include the marker `goal:complete` and a non-empty `Completion evidence:` section with these labeled fields: `Verification loop:`, `Environment validation:`, `Boundary/fuzz checks:`, and `Independent evaluator:`.",
    "- Each completion evidence field must name the commands, observations, evaluator findings, or a concrete not-applicable/blocker reason.",
    "- If verification cannot be run, name the residual risk and do not use `goal:complete` unless the objective explicitly allows unverified completion.",
    "- If blocked by missing input, external failure, or the continuation budget, include `goal:blocked` and explain the blocker.",
  ].join("\n")
}

function continueMessage(goal: GoalState) {
  return [
    "Continue working on the active goal.",
    "",
    "<goal_objective>",
    goal.objective,
    "</goal_objective>",
    "",
    "Before doing anything else, run a fresh status check. Then choose the next experiment/action, record the observation, and keep going through the verification loop, isolated environment validation, boundary/fuzz checks, and independent evaluator pass until the strict completion protocol is met or the goal is blocked.",
  ].join("\n")
}

async function continueGoal(input: PluginInput, goals: Map<string, GoalState>, sessionID: string) {
  const goal = await loadGoal(input, goals, sessionID)
  if (!goal || goal.status !== "active" || goal.pending) return
  if (goal.continuations >= MAX_CONTINUATIONS) {
    goal.status = "blocked"
    return
  }

  goal.pending = true
  await new Promise((resolve) => setTimeout(resolve, CONTINUE_DELAY_MS))

  const current = goals.get(sessionID)
  if (!current || current.status !== "active") {
    goal.pending = false
    return
  }

  current.continuations += 1
  try {
    await input.client.session.promptAsync({
      path: { id: sessionID },
      body: {
        agent: GOAL_AGENT,
        parts: [
          {
            type: "text",
            text: continueMessage(current),
            metadata: { [META_CONTINUE]: true },
          },
        ],
      },
    })
  } catch {
    current.status = "blocked"
  } finally {
    current.pending = false
  }
}

function markFromEvent(goals: Map<string, GoalState>, event: Event) {
  if (event.type === "message.updated") {
    const goal = goals.get(event.properties.info.sessionID)
    if (!goal || goal.status !== "active") return
    if (event.properties.info.role === "assistant" && event.properties.info.error) goal.status = "blocked"
    return
  }

  if (event.type !== "message.part.updated") return
  const part = event.properties.part
  if (part.type !== "text") return
  const status = part.metadata?.[META_STATUS]
  if (status === "cleared") {
    goals.delete(part.sessionID)
    return
  }

  const objective = part.metadata?.[META_OBJECTIVE]
  if (typeof objective === "string" && objective.trim()) {
    goals.set(part.sessionID, {
      objective,
      continuations: 0,
      pending: false,
      status: "active",
    })
    return
  }

  const goal = goals.get(part.sessionID)
  if (!goal || goal.status !== "active") return

  const text = part.text.toLowerCase()
  if (text.includes("goal:complete") && hasCompletionEvidence(part.text)) goal.status = "complete"
  if (text.includes("goal:blocked")) goal.status = "blocked"
}

async function loadGoal(input: PluginInput, goals: Map<string, GoalState>, sessionID: string) {
  const current = goals.get(sessionID)
  if (current) return current

  try {
    const response = await input.client.session.messages({
      path: { id: sessionID },
    })
    const goal = restoreGoal(response.data ?? [])
    if (goal) goals.set(sessionID, goal)
    return goal
  } catch {
    return
  }
}

function restoreGoal(messages: MessageWithParts[]) {
  const markers: GoalMarker[] = messages.flatMap((message, messageIndex) =>
    message.parts.flatMap((part): GoalMarker[] => {
      if (part.type !== "text") return []
      const command = part.metadata?.[META_COMMAND]
      const objective = part.metadata?.[META_OBJECTIVE]
      const status = part.metadata?.[META_STATUS]
      if (status === "cleared") return [{ messageIndex, kind: "clear" as const }]
      if (command === "start" && typeof objective === "string" && objective.trim()) {
        return [{ messageIndex, kind: "start" as const, objective }]
      }
      return []
    }),
  )
  const latest = markers.at(-1)
  if (!latest || latest.kind === "clear") return

  const afterStart = messages.slice(latest.messageIndex)
  const goal: GoalState = {
    objective: latest.objective,
    continuations: countContinuations(afterStart),
    pending: false,
    status: "active",
  }

  for (const message of afterStart) {
    if (message.info.role === "assistant" && message.info.error) goal.status = "blocked"
    const text = message.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n")
    const lower = text.toLowerCase()
    if (lower.includes("goal:complete") && hasCompletionEvidence(text)) goal.status = "complete"
    if (lower.includes("goal:blocked")) goal.status = "blocked"
  }
  return goal
}

function countContinuations(messages: MessageWithParts[]) {
  return messages
    .flatMap((message) => message.parts)
    .filter((part) => part.type === "text" && part.metadata?.[META_CONTINUE] === true).length
}

function hasCompletionEvidence(text: string) {
  const evidence = /completion evidence[:：]([\s\S]*)/i
    .exec(text)?.[1]
    ?.replace(/goal:(complete|blocked)/gi, "")
    .trim()
  if (!evidence) return false
  return COMPLETION_EVIDENCE_SECTIONS.every((section) => section.test(evidence))
}
