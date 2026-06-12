import { describe, expect, test } from "bun:test"
import type { Agent, Message, Part } from "@opencode-ai/sdk/v2/client"
import { summarizeSessionSubagents } from "./session-subagent-summary"

const agents: Agent[] = [
  { name: "build", mode: "primary", permission: [], options: {} },
  { name: "explore", mode: "subagent", color: "#00f", permission: [], options: {} },
  { name: "plan", mode: "subagent", permission: [], options: {} },
]

const user = (id: string): Message => ({
  id,
  sessionID: "ses_1",
  role: "user",
  time: { created: 1 },
  agent: "build",
  model: { providerID: "openai", modelID: "gpt" },
})

const assistant = (id: string, agent = "build"): Message => ({
  id,
  sessionID: "ses_1",
  role: "assistant",
  time: { created: 2 },
  parentID: "user",
  modelID: "gpt",
  providerID: "openai",
  mode: "build",
  agent,
  path: { cwd: "/", root: "/" },
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
})

const agentPart = (messageID: string, name: string): Part => ({
  id: `part_${messageID}_${name}`,
  sessionID: "ses_1",
  messageID,
  type: "agent",
  name,
})

const taskPart = (messageID: string, name: string, status: "running" | "completed" | "error"): Part => ({
  id: `part_${messageID}_${name}`,
  sessionID: "ses_1",
  messageID,
  type: "tool",
  callID: `call_${name}`,
  tool: "task",
  state:
    status === "running"
      ? { status: "running", input: { subagent_type: name }, time: { start: 1 } }
      : status === "completed"
        ? { status: "completed", input: { subagent_type: name }, output: "", title: "", metadata: {}, time: { start: 1, end: 2 } }
        : { status: "error", input: { subagent_type: name }, error: "failed", time: { start: 1, end: 2 } },
})

describe("summarizeSessionSubagents", () => {
  test("collects mentioned and running task subagents", () => {
    const summary = summarizeSessionSubagents({
      agents,
      messages: [user("u1"), assistant("a1")],
      parts: {
        u1: [agentPart("u1", "explore")],
        a1: [taskPart("a1", "plan", "running")],
      },
    })

    expect(summary.activeCount).toBe(1)
    expect(summary.items.map((item) => [item.name, item.status])).toEqual([
      ["plan", "running"],
      ["explore", "mentioned"],
    ])
  })

  test("does not show the primary assistant unless it was explicitly involved", () => {
    const summary = summarizeSessionSubagents({
      agents,
      messages: [user("u1"), assistant("a1", "build"), assistant("a2", "explore")],
      parts: {},
    })

    expect(summary.items.map((item) => item.name)).toEqual(["explore"])
  })
})
