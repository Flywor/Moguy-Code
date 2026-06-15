export * as WorkflowAgent from "./agent"

import { Context, Effect, Layer, Schema } from "effect"
import { AgentV2 } from "../agent"
import { Location } from "../location"
import { AbsolutePath } from "../schema"
import { Prompt } from "../session/prompt"
import { SessionV2 } from "../session"
import { SessionMessage } from "../session/message"
import { SessionSchema } from "../session/schema"

export const Input = Schema.Struct({
  runID: Schema.String,
  sessionID: SessionSchema.ID,
  prompt: Schema.String,
  options: Schema.Record(Schema.String, Schema.Unknown),
  workspace: Schema.String.pipe(Schema.optional),
}).annotate({ identifier: "WorkflowAgent.Input" })
export type Input = typeof Input.Type

export interface Interface {
  readonly run: (input: Input) => Effect.Effect<unknown, unknown>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/WorkflowAgent") {}

const ref: { current?: Interface } = {}

export const layerFromRef = Layer.succeed(
  Service,
  Service.of({
    run: (input) =>
      Effect.suspend(() =>
        ref.current ? ref.current.run(input) : Effect.fail(new Error("Workflow agent bridge is not installed")),
      ),
  }),
)

export const install = (agent: Interface) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const previous = ref.current
      ref.current = agent
      return previous
    }),
    (previous) =>
      Effect.sync(() => {
        if (ref.current !== agent) return
        if (previous === undefined) {
          delete ref.current
          return
        }
        ref.current = previous
      }),
  ).pipe(Effect.asVoid)

export const layerFromSession = Layer.effect(
  Service,
  Effect.gen(function* () {
    const sessions = yield* SessionV2.Service

    return Service.of(fromSession(sessions))
  }),
)

export const sessionBridgeLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* install(fromSession(yield* SessionV2.Service))
  }),
)

export function fromSession(sessions: SessionV2.Interface): Interface {
  return {
    run: Effect.fn("WorkflowAgent.run")(function* (input: Input) {
      const parent = yield* sessions.get(input.sessionID)
      const child = yield* sessions.create({
        agent: typeof input.options.agent === "string" ? AgentV2.ID.make(input.options.agent) : undefined,
        location: Location.Ref.make({
          directory: input.workspace ? AbsolutePath.make(input.workspace) : parent.location.directory,
          workspaceID: parent.location.workspaceID,
        }),
      })
      yield* sessions.prompt({
        sessionID: child.id,
        prompt: new Prompt({
          text: [
            "You are a workflow subagent.",
            `Parent session: ${input.sessionID}`,
            `Workflow run: ${input.runID}`,
            "",
            input.prompt,
          ].join("\n"),
        }),
        resume: false,
      })
      yield* sessions.resume(child.id)
      return finalAssistantText(yield* sessions.context(child.id))
    }),
  }
}

function finalAssistantText(messages: readonly SessionMessage.Message[]) {
  const message = messages.toReversed().find((message) => message.type === "assistant")
  if (!message || message.type !== "assistant") return null
  const text = message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n")
  return text || null
}
