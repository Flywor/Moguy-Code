import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260615013356_workflow_workspace_lease",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`workflow_run\` ADD \`workspace_managed\` integer DEFAULT false NOT NULL;`)
      yield* tx.run(`ALTER TABLE \`workflow_run\` ADD \`workspace_remove_on_finish\` integer DEFAULT false NOT NULL;`)
      yield* tx.run(`ALTER TABLE \`workflow_run\` ADD \`workspace_remove_on_cancel\` integer DEFAULT false NOT NULL;`)
      yield* tx.run(`ALTER TABLE \`workflow_run\` ADD \`workspace_force_remove\` integer DEFAULT true NOT NULL;`)
    })
  },
} satisfies DatabaseMigration.Migration
