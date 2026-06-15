export * as MemoryTool from "./memory"

import { ToolFailure } from "@opencode-ai/llm"
import { Effect, Layer, Schema } from "effect"
import { Location } from "../location"
import { PermissionV2 } from "../permission"
import { SessionMemorySearch } from "../session/memory-search"
import { Tool } from "./tool"
import { Tools } from "./tools"

export const name = "memory"

const SearchInput = Schema.Struct({
  operation: Schema.Literal("search").pipe(Schema.optional).annotate({
    description: "Search markdown memory files.",
  }),
  query: Schema.String.annotate({ description: "Search query over markdown memory bodies." }),
  scope: Schema.Literals(["global", "projects", "sessions", "cc"]).pipe(Schema.optional).annotate({
    description: "Optional memory scope filter.",
  }),
  scopeID: Schema.String.pipe(Schema.optional).annotate({
    description: "Optional scope id filter, such as a project id or session id.",
  }),
  type: Schema.String.pipe(Schema.optional).annotate({
    description: "Optional memory type filter, such as checkpoint, memory, progress, notes, feedback, project, reference, or user.",
  }),
  limit: Schema.Number.pipe(Schema.optional).annotate({ description: "Maximum results. Defaults to 10." }),
})
const RootInput = Schema.Struct({
  operation: Schema.Literal("root").annotate({ description: "Return the root directory for markdown memory files." }),
})
const ReconcileInput = Schema.Struct({
  operation: Schema.Literal("reconcile").annotate({ description: "Re-index markdown memory files into SQLite FTS." }),
})
const WriteInput = Schema.Struct({
  operation: Schema.Literal("write").annotate({
    description: "Write a markdown memory file and index it immediately.",
  }),
  scope: Schema.Literals(["global", "projects", "sessions"]).pipe(Schema.optional).annotate({
    description: "Memory scope. Defaults to projects.",
  }),
  scopeID: Schema.String.pipe(Schema.optional).annotate({
    description: "Scope id. Defaults to the current project id for projects and current session id for sessions.",
  }),
  key: Schema.String.annotate({
    description: "Memory key under the scope, without .md, for example MEMORY, notes, or workflows/release.",
  }),
  body: Schema.String.annotate({ description: "Markdown body to write." }),
  mode: Schema.Literals(["replace", "append"]).pipe(Schema.optional).annotate({
    description: "Write mode. Defaults to replace.",
  }),
})

export const Input = Schema.Union([SearchInput, RootInput, ReconcileInput, WriteInput])

export const Output = Schema.Struct({
  message: Schema.String,
  root: Schema.String.pipe(Schema.optional),
  results: Schema.Array(SessionMemorySearch.SearchResult).pipe(Schema.optional),
  path: Schema.String.pipe(Schema.optional),
  scope: Schema.String.pipe(Schema.optional),
  scopeID: Schema.String.pipe(Schema.optional),
  key: Schema.String.pipe(Schema.optional),
  type: Schema.String.pipe(Schema.optional),
  existed: Schema.Boolean.pipe(Schema.optional),
  indexed: Schema.Finite.pipe(Schema.optional),
  pruned: Schema.Finite.pipe(Schema.optional),
})
export type Output = typeof Output.Type

export const toModelOutput = (output: Output) => {
  if (output.root) return `${output.message}\n${output.root}`
  if (output.path) {
    return [
      output.message,
      `Path: ${output.path}`,
      `Scope: ${output.scope}${output.scopeID ? `/${output.scopeID}` : ""}, Type: ${output.type}`,
      output.existed ? "Existing file was updated." : "New memory file was created.",
    ].join("\n")
  }
  if (output.indexed !== undefined || output.pruned !== undefined) return output.message
  const results = output.results ?? []
  if (results.length === 0) {
    return [
      "Memory search returned 0 results.",
      "",
      "Try again with fewer, more distinctive terms. For exact literals containing punctuation, grep the memory directory directly.",
    ].join("\n")
  }
  return [
    `Found ${results.length} memory result${results.length === 1 ? "" : "s"} (BM25-ranked, best first).`,
    "Use Read on a result path when you need the full body.",
    "",
    ...results.flatMap((result) => [
      `### ${result.path}`,
      `Scope: ${result.scope}${result.scopeID ? `/${result.scopeID}` : ""}, Type: ${result.type}, Score: ${result.score.toFixed(3)}`,
      result.snippet,
      "",
    ]),
  ].join("\n")
}

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const tools = yield* Tools.Service
    const memory = yield* SessionMemorySearch.Service
    const location = yield* Location.Service
    const permission = yield* PermissionV2.Service

    yield* tools
      .register({
        [name]: Tool.make({
          description: [
            "Search durable markdown memory using SQLite FTS5/BM25.",
            "Use operation=root to locate the memory tree, operation=write to store durable markdown memory, and operation=reconcile to re-index files after external edits.",
            "Search before guessing from recent context. Prefer 1-3 distinctive terms; snippets are truncated, so use Read on returned paths for full content.",
          ].join(" "),
          input: Input,
          output: Output,
          toModelOutput: ({ output }) => [{ type: "text", text: toModelOutput(output) }],
          execute: (input, context) =>
            Effect.gen(function* () {
              yield* permission.assert({
                action: name,
                resources: ["*"],
                save: ["*"],
                sessionID: context.sessionID,
                agent: context.agent,
                source: { type: "tool", messageID: context.assistantMessageID, callID: context.toolCallID },
              })
              if (input.operation === "root") {
                const root = yield* memory.root()
                return { message: "Memory root:", root }
              }
              if (input.operation === "reconcile") {
                const result = yield* memory.reconcile()
                return {
                  message: `Memory reconcile complete: indexed ${result.indexed}, pruned ${result.pruned}.`,
                  indexed: result.indexed,
                  pruned: result.pruned,
                }
              }
              if (input.operation === "write") {
                const scope = input.scope ?? "projects"
                if (!safeMemoryComponent(input.key))
                  return yield* new ToolFailure({ message: `Invalid memory key: ${input.key}` })
                if (input.scopeID && !safeMemoryComponent(input.scopeID))
                  return yield* new ToolFailure({ message: `Invalid memory scopeID: ${input.scopeID}` })
                const result = yield* memory.write({
                  scope,
                  scopeID:
                    input.scopeID ??
                    (scope === "projects" ? location.project.id : scope === "sessions" ? context.sessionID : undefined),
                  key: input.key,
                  body: input.body,
                  mode: input.mode,
                })
                return { message: "Memory file written and indexed.", ...result }
              }
              const results = yield* memory.search({
                query: input.query,
                scope: input.scope,
                scopeID: input.scopeID,
                type: input.type,
                limit: input.limit,
              })
              return {
                message: `Found ${results.length} memory result${results.length === 1 ? "" : "s"}.`,
                results,
              }
            }).pipe(
              Effect.mapError((error) =>
                error instanceof ToolFailure ? error : new ToolFailure({ message: "Unable to use memory" }),
              ),
            ),
        }),
      })
      .pipe(Effect.orDie)
  }),
)

function safeMemoryComponent(value: string) {
  if (value.startsWith("/")) return false
  return value.split("/").every((segment) => segment !== ".." && segment.length > 0)
}
