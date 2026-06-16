import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { httpClient } from "@opencode-ai/core/effect/layer-node-platform"
import { serviceUse } from "@opencode-ai/core/effect/service-use"
import type * as SDK from "@opencode-ai/sdk/v2"
import { Effect, Layer, Schema, Context } from "effect"
import { SessionID } from "@/session/schema"

export type Api = {
  create: string
  sync: (shareID: string) => string
  remove: (shareID: string) => string
  data: (shareID: string) => string
}

export type Req = {
  headers: Record<string, string>
  api: Api
  baseUrl: string
}

const ShareSchema = Schema.Struct({
  id: Schema.String,
  url: Schema.String,
  secret: Schema.String,
})
export type Share = typeof ShareSchema.Type

export interface Interface {
  readonly init: () => Effect.Effect<void, unknown>
  readonly url: () => Effect.Effect<string, unknown>
  readonly request: () => Effect.Effect<Req, unknown>
  readonly create: (sessionID: SessionID) => Effect.Effect<Share, unknown>
  readonly remove: (sessionID: SessionID) => Effect.Effect<void, unknown>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ShareNext") {}

export const use = serviceUse(Service)

export const layer = Layer.succeed(
  Service,
  Service.of({
    init: () => Effect.void,
    url: () => Effect.succeed(""),
    request: () =>
      Effect.succeed({
        headers: {},
        api: {
          create: "",
          sync: () => "",
          remove: () => "",
          data: () => "",
        },
        baseUrl: "",
      }),
    create: () =>
      Effect.fail(new Error("Sharing is not available")),
    remove: () => Effect.void,
  }),
)

export const defaultLayer = layer

export const node = LayerNode.make(layer, [httpClient])

export * as ShareNext from "./share-next"
