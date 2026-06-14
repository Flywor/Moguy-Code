import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260613174318_session_memory",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_memory\` (
          \`session_id\` text PRIMARY KEY,
          \`project_id\` text NOT NULL,
          \`source_message_id\` text NOT NULL,
          \`summary\` text NOT NULL,
          \`recent\` text NOT NULL,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          CONSTRAINT \`fk_session_memory_session_id_session_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session\`(\`id\`) ON DELETE CASCADE,
          CONSTRAINT \`fk_session_memory_project_id_project_id_fk\` FOREIGN KEY (\`project_id\`) REFERENCES \`project\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`CREATE INDEX \`session_memory_project_updated_idx\` ON \`session_memory\` (\`project_id\`,\`time_updated\`);`)
      yield* tx.run(`CREATE INDEX \`session_memory_source_message_idx\` ON \`session_memory\` (\`source_message_id\`);`)
    })
  },
} satisfies DatabaseMigration.Migration
