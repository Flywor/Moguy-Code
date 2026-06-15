export * as SessionMaintenance from "./maintenance"

import { Context, Effect, Layer, Schema } from "effect"
import { SessionSchema } from "./schema"

export const Kind = Schema.Literals(["dream", "distill"])
export type Kind = typeof Kind.Type

export const Request = Schema.Struct({
  session: SessionSchema.Info,
  kind: Kind,
  prompt: Schema.String,
}).annotate({ identifier: "SessionMaintenance.Request" })
export type Request = typeof Request.Type

export interface Interface {
  readonly request: (input: Request) => Effect.Effect<void, unknown>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SessionMaintenance") {}

const ref: { current?: Interface } = {}

export const layerFromRef = Layer.succeed(
  Service,
  Service.of({
    request: (input) =>
      Effect.suspend(() =>
        ref.current ? ref.current.request(input) : Effect.fail(new Error("Session maintenance bridge is not installed")),
      ),
  }),
)

export const install = (service: Interface) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const previous = ref.current
      ref.current = service
      return previous
    }),
    (previous) =>
      Effect.sync(() => {
        if (ref.current !== service) return
        if (previous === undefined) {
          delete ref.current
          return
        }
        ref.current = previous
      }),
  ).pipe(Effect.asVoid)
