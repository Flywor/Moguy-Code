import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260614163331_workflow_runtime",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`workflow_run\` (
          \`id\` text PRIMARY KEY,
          \`session_id\` text NOT NULL,
          \`name\` text NOT NULL,
          \`status\` text NOT NULL,
          \`workspace\` text,
          \`script_sha\` text NOT NULL,
          \`running\` integer DEFAULT 0 NOT NULL,
          \`succeeded\` integer DEFAULT 0 NOT NULL,
          \`failed\` integer DEFAULT 0 NOT NULL,
          \`current_phase\` text,
          \`args\` text,
          \`result\` text,
          \`error\` text,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          CONSTRAINT \`fk_workflow_run_session_id_session_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`CREATE INDEX \`workflow_run_session_idx\` ON \`workflow_run\` (\`session_id\`,\`time_created\`);`)
      yield* tx.run(`CREATE INDEX \`workflow_run_status_idx\` ON \`workflow_run\` (\`status\`);`)
    })
  },
} satisfies DatabaseMigration.Migration
