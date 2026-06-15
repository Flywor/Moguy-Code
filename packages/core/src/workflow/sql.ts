import { index, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { SessionTable } from "../session/sql"
import type { SessionSchema } from "../session/schema"
import { Timestamps } from "../database/schema.sql"

export type WorkflowRunStatus = "running" | "completed" | "failed" | "cancelled"

export const WorkflowRunTable = sqliteTable(
  "workflow_run",
  {
    id: text().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    name: text().notNull(),
    status: text().$type<WorkflowRunStatus>().notNull(),
    workspace: text(),
    workspace_managed: integer({ mode: "boolean" }).notNull().default(false),
    workspace_remove_on_finish: integer({ mode: "boolean" }).notNull().default(false),
    workspace_remove_on_cancel: integer({ mode: "boolean" }).notNull().default(false),
    workspace_force_remove: integer({ mode: "boolean" }).notNull().default(true),
    script_sha: text().notNull(),
    running: integer().notNull().default(0),
    succeeded: integer().notNull().default(0),
    failed: integer().notNull().default(0),
    current_phase: text(),
    args: text({ mode: "json" }).$type<unknown>(),
    result: text({ mode: "json" }).$type<unknown>(),
    error: text(),
    ...Timestamps,
  },
  (table) => [
    index("workflow_run_session_idx").on(table.session_id, table.time_created),
    index("workflow_run_status_idx").on(table.status),
  ],
)
