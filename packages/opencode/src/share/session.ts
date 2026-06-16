import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import { Effect, Layer, Context } from "effect"

export interface Interface {
  readonly create: (input?: Session.CreateInput) => Effect.Effect<Session.Info>
  readonly share: (sessionID: SessionID) => Effect.Effect<{ url: string }, unknown>
  readonly unshare: (sessionID: SessionID) => Effect.Effect<void, unknown>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionShare") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const session = yield* Session.Service

    return Service.of({
      create: (input) => session.create(input),
      share: () => Effect.succeed({ url: "" }),
      unshare: () => Effect.void,
    })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(Session.defaultLayer))

export const node = LayerNode.make(layer, [Session.node])

export * as SessionShare from "./session"
