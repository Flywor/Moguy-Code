import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260614120000_task_memory_runtime",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_task\` (
          \`session_id\` text NOT NULL,
          \`id\` text NOT NULL,
          \`parent_task_id\` text,
          \`status\` text NOT NULL,
          \`summary\` text NOT NULL,
          \`owner\` text,
          \`time_ended\` integer,
          \`time_cleanup\` integer,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          CONSTRAINT \`session_task_pk\` PRIMARY KEY(\`session_id\`, \`id\`),
          CONSTRAINT \`fk_session_task_session_id_session_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`CREATE INDEX \`session_task_session_status_idx\` ON \`session_task\` (\`session_id\`,\`status\`);`)
      yield* tx.run(`CREATE INDEX \`session_task_session_owner_idx\` ON \`session_task\` (\`session_id\`,\`owner\`);`)
      yield* tx.run(`CREATE INDEX \`session_task_cleanup_idx\` ON \`session_task\` (\`time_cleanup\`);`)
      yield* tx.run(`
        CREATE TABLE \`session_task_event\` (
          \`id\` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
          \`session_id\` text NOT NULL,
          \`task_id\` text NOT NULL,
          \`at\` integer NOT NULL,
          \`kind\` text NOT NULL,
          \`summary\` text,
          CONSTRAINT \`fk_session_task_event_session_id_session_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(
        `CREATE INDEX \`session_task_event_task_idx\` ON \`session_task_event\` (\`session_id\`,\`task_id\`,\`at\`);`,
      )
      yield* tx.run(`CREATE INDEX \`session_task_event_session_idx\` ON \`session_task_event\` (\`session_id\`,\`at\`);`)
      yield* tx.run(`
        CREATE TABLE \`memory_fts\` (
          \`id\` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
          \`path\` text NOT NULL,
          \`scope\` text NOT NULL,
          \`scope_id\` text DEFAULT '' NOT NULL,
          \`type\` text NOT NULL,
          \`body\` text NOT NULL,
          \`fingerprint\` text NOT NULL,
          \`last_indexed_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`CREATE UNIQUE INDEX \`memory_fts_path_idx\` ON \`memory_fts\` (\`path\`);`)
      yield* tx.run(`CREATE INDEX \`memory_fts_scope_idx\` ON \`memory_fts\` (\`scope\`,\`scope_id\`);`)
      yield* tx.run(`CREATE INDEX \`memory_fts_type_idx\` ON \`memory_fts\` (\`type\`);`)
      yield* tx.run(`
        CREATE VIRTUAL TABLE \`memory_fts_idx\` USING fts5(
          \`body\`,
          content='memory_fts',
          content_rowid='id',
          tokenize='unicode61 remove_diacritics 1'
        );
      `)
      yield* tx.run(`
        CREATE TRIGGER \`memory_fts_ai\` AFTER INSERT ON \`memory_fts\` BEGIN
          INSERT INTO \`memory_fts_idx\`(\`rowid\`, \`body\`) VALUES (NEW.\`id\`, NEW.\`body\`);
        END;
      `)
      yield* tx.run(`
        CREATE TRIGGER \`memory_fts_ad\` AFTER DELETE ON \`memory_fts\` BEGIN
          INSERT INTO \`memory_fts_idx\`(\`memory_fts_idx\`, \`rowid\`, \`body\`) VALUES('delete', OLD.\`id\`, OLD.\`body\`);
        END;
      `)
      yield* tx.run(`
        CREATE TRIGGER \`memory_fts_au\` AFTER UPDATE ON \`memory_fts\` BEGIN
          INSERT INTO \`memory_fts_idx\`(\`memory_fts_idx\`, \`rowid\`, \`body\`) VALUES('delete', OLD.\`id\`, OLD.\`body\`);
          INSERT INTO \`memory_fts_idx\`(\`rowid\`, \`body\`) VALUES (NEW.\`id\`, NEW.\`body\`);
        END;
      `)
    })
  },
} satisfies DatabaseMigration.Migration
