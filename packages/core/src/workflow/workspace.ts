export * as WorkflowWorkspace from "./workspace"

import path from "node:path"
import { Context, Effect, Layer, Schema } from "effect"
import { Global } from "../global"
import { Location } from "../location"
import { ProjectCopy } from "../project/copy"
import { AbsolutePath } from "../schema"

export const Lease = Schema.Struct({
  directory: AbsolutePath,
  managed: Schema.Boolean,
  removeOnFinish: Schema.Boolean,
  removeOnCancel: Schema.Boolean,
  forceRemove: Schema.Boolean,
}).annotate({ identifier: "WorkflowWorkspace.Lease" })
export type Lease = typeof Lease.Type

export type CreateInput = {
  readonly runID: string
  readonly sourceDirectory?: string
  readonly parentDirectory?: string
  readonly name?: string
  readonly removeOnFinish?: boolean
  readonly removeOnCancel?: boolean
  readonly forceRemove?: boolean
}

export interface Interface {
  readonly create: (input: CreateInput) => Effect.Effect<Lease, unknown>
  readonly release: (input: { readonly directory: string; readonly force: boolean }) => Effect.Effect<void, unknown>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/WorkflowWorkspace") {}

export const layerFromProjectCopy = Layer.effect(
  Service,
  Effect.gen(function* () {
    const copy = yield* ProjectCopy.Service
    const global = yield* Global.Service
    const location = yield* Location.Service

    const create = Effect.fn("WorkflowWorkspace.create")(function* (input: CreateInput) {
      const created = yield* copy.create({
        projectID: location.project.id,
        strategy: ProjectCopy.StrategyID.make("git_worktree"),
        sourceDirectory: AbsolutePath.make(input.sourceDirectory ?? location.directory),
        directory: AbsolutePath.make(input.parentDirectory ?? path.join(global.data, "workflow-worktrees")),
        name: input.name ?? input.runID,
      })
      return {
        directory: created.directory,
        managed: true,
        removeOnFinish: input.removeOnFinish ?? false,
        removeOnCancel: input.removeOnCancel ?? true,
        forceRemove: input.forceRemove ?? true,
      }
    })

    const release = Effect.fn("WorkflowWorkspace.release")(function* (input: {
      readonly directory: string
      readonly force: boolean
    }) {
      yield* copy.remove({
        projectID: location.project.id,
        directory: AbsolutePath.make(input.directory),
        force: input.force,
      })
    })

    return Service.of({ create, release })
  }),
)
