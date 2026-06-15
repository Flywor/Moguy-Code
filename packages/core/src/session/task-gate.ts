export * as SessionTaskGate from "./task-gate"

import { Effect } from "effect"
import { SessionSchema } from "./schema"
import { SessionTask } from "./task"

export const MAX_MAIN_REACT = 3

export type Decision =
  | { readonly needReentry: false; readonly capExceeded: false; readonly incompleteTasks: readonly [] }
  | {
      readonly needReentry: true
      readonly capExceeded: false
      readonly incompleteTasks: readonly string[]
      readonly reentryText: string
    }
  | { readonly needReentry: false; readonly capExceeded: true; readonly incompleteTasks: readonly string[] }

export const decide = Effect.fn("SessionTaskGate.decide")(function* (input: {
  readonly tasks: SessionTask.Interface
  readonly sessionID: SessionSchema.ID
  readonly reactCount: number
  readonly maxReact?: number
}) {
  const tasks = yield* input.tasks
    .list({ sessionID: input.sessionID, includeTerminal: false })
    .pipe(Effect.orElseSucceed(() => []))
  const actionable = tasks.filter((task) => task.status === "open" || task.status === "in_progress")
  if (actionable.length === 0)
    return { needReentry: false, capExceeded: false, incompleteTasks: [] } satisfies Decision
  if (input.reactCount >= (input.maxReact ?? MAX_MAIN_REACT)) {
    return {
      needReentry: false,
      capExceeded: true,
      incompleteTasks: actionable.map((task) => task.id),
    } satisfies Decision
  }
  return {
    needReentry: true,
    capExceeded: false,
    incompleteTasks: actionable.map((task) => task.id),
    reentryText: [
      "<system-reminder>",
      "You are about to finish, but these tasks in this session are still unfinished:",
      ...actionable.map((task) => `- ${task.id} (${task.status}): ${task.summary}`),
      "For each task: finish the work and mark it done, or abandon it with a reason if it is genuinely no longer needed.",
      "Then continue or respond.",
      "</system-reminder>",
    ].join("\n"),
  } satisfies Decision
})
