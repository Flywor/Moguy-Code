import type { Agent, Message, Part, ToolPart } from "@opencode-ai/sdk/v2/client"
import { agentColor } from "@/utils/agent"

export type SubagentStatus = "running" | "error" | "completed" | "responded" | "mentioned"

export type SessionSubagentItem = {
  name: string
  status: SubagentStatus
  color: string
}

export type SessionSubagentSummary = {
  items: SessionSubagentItem[]
  activeCount: number
}

type Input = {
  messages: readonly Message[]
  parts: Record<string, readonly Part[] | undefined>
  agents: readonly Agent[]
}

const statusRank: Record<SubagentStatus, number> = {
  running: 4,
  error: 3,
  completed: 2,
  responded: 1,
  mentioned: 0,
}

function taskAgent(part: ToolPart) {
  if (part.tool !== "task") return
  const value = part.state.input.subagent_type
  return typeof value === "string" && value.length > 0 ? value : undefined
}

function taskStatus(part: ToolPart): SubagentStatus {
  if (part.state.status === "pending" || part.state.status === "running") return "running"
  if (part.state.status === "error") return "error"
  return "completed"
}

export function summarizeSessionSubagents(input: Input): SessionSubagentSummary {
  const agentByName = new Map(input.agents.map((agent) => [agent.name, agent]))
  const items = new Map<string, SessionSubagentItem>()

  const upsert = (name: string, status: SubagentStatus) => {
    const existing = items.get(name)
    if (existing && statusRank[existing.status] >= statusRank[status]) return
    const agent = agentByName.get(name)
    items.set(name, {
      name,
      status,
      color: agentColor(name, agent?.color),
    })
  }

  for (const message of input.messages) {
    for (const part of input.parts[message.id] ?? []) {
      if (part.type === "agent") {
        upsert(part.name, "mentioned")
        continue
      }
      if (part.type !== "tool") continue
      const name = taskAgent(part)
      if (!name) continue
      upsert(name, taskStatus(part))
    }

    if (message.role !== "assistant") continue
    const agent = agentByName.get(message.agent)
    if (agent?.mode === "primary" && !items.has(message.agent)) continue
    upsert(message.agent, "responded")
  }

  const sorted = Array.from(items.values()).sort((a, b) => {
    const rank = statusRank[b.status] - statusRank[a.status]
    return rank === 0 ? a.name.localeCompare(b.name) : rank
  })

  return {
    items: sorted,
    activeCount: sorted.filter((item) => item.status === "running").length,
  }
}
