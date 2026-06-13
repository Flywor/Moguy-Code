import { describe, expect, it } from "bun:test"
import type { Event, Part } from "@opencode-ai/sdk"
import type { PluginInput } from "@opencode-ai/plugin"
import { GoalPlugin } from "../../src/plugin/goal"

type PromptRequest = {
  path: { id: string }
  body: {
    agent?: string
    parts: Array<{
      type: "text"
      text: string
      metadata?: Record<string, unknown>
    }>
  }
}
type MessageWithParts = {
  info: {
    id: string
    sessionID: string
    role: "user" | "assistant"
  }
  parts: Part[]
}

function createInput(prompted: PromptRequest[], messages: MessageWithParts[] = []) {
  return {
    client: {
      session: {
        messages: async () => ({ data: messages }),
        promptAsync: async (request: PromptRequest) => {
          prompted.push(request)
        },
      },
    },
  } as unknown as PluginInput
}

function output(text = "") {
  return {
    parts: [{ type: "text", text } as unknown as Part],
  }
}

function textOf(parts: Part[]) {
  const part = parts.find((item) => item.type === "text")
  return part?.type === "text" ? part.text : ""
}

describe("GoalPlugin", () => {
  it("starts a goal, injects goal context, and auto-continues until complete evidence exists", async () => {
    const prompted: PromptRequest[] = []
    const hooks = await GoalPlugin(createInput(prompted))
    const command = hooks["command.execute.before"]
    const system = hooks["experimental.chat.system.transform"]
    const event = hooks.event
    expect(command).toBeDefined()
    expect(system).toBeDefined()
    expect(event).toBeDefined()

    const commandOutput = output("ship it")
    await command!({ command: "goal", sessionID: "session-1", arguments: "ship it" }, commandOutput)
    expect(textOf(commandOutput.parts)).toContain("Objective: ship it")
    const metadata = commandOutput.parts.find((item) => item.type === "text")?.metadata
    expect(metadata?.goal_objective).toBe("ship it")

    const systemOutput = { system: [] as string[] }
    await system!({ sessionID: "session-1", model: {} as never }, systemOutput)
    expect(systemOutput.system.join("\n")).toContain("<goal_objective>\nship it\n</goal_objective>")
    expect(systemOutput.system.join("\n")).toContain("scientific experiment loop")

    await event!({
      event: {
        type: "session.idle",
        properties: { sessionID: "session-1" },
      } as Event,
    })
    expect(prompted).toHaveLength(1)
    expect(prompted[0]?.path.id).toBe("session-1")
    expect(prompted[0]?.body.agent).toBe("build")
    expect(prompted[0]?.body.parts[0]?.text).toContain("Continue working on the active goal")

    await event!({
      event: {
        type: "message.part.updated",
        properties: {
          part: {
            type: "text",
            sessionID: "session-1",
            text: "Completion evidence: tests passed.\ngoal:complete",
          },
        },
      } as Event,
    })
    await event!({
      event: {
        type: "session.idle",
        properties: { sessionID: "session-1" },
      } as Event,
    })
    expect(prompted).toHaveLength(1)
  })

  it("restores a goal from session history before auto-continuing", async () => {
    const prompted: PromptRequest[] = []
    const messages: MessageWithParts[] = [
      {
        info: { id: "msg-1", sessionID: "session-1", role: "user" },
        parts: [
          {
            type: "text",
            text: "Goal mode is active for this session.\n\nObjective: recover me",
            metadata: {
              goal_command: "start",
              goal_objective: "recover me",
              goal_status: "active",
            },
          } as unknown as Part,
        ],
      },
    ]
    const hooks = await GoalPlugin(createInput(prompted, messages))

    await hooks.event!({
      event: {
        type: "session.idle",
        properties: { sessionID: "session-1" },
      } as Event,
    })

    expect(prompted).toHaveLength(1)
    expect(prompted[0]?.body.agent).toBe("build")
    expect(prompted[0]?.body.parts[0]?.text).toContain("recover me")
  })

  it("keeps the goal active when completion evidence is empty", async () => {
    const prompted: PromptRequest[] = []
    const hooks = await GoalPlugin(createInput(prompted))

    await hooks["command.execute.before"]!({ command: "goal", sessionID: "session-1", arguments: "prove it" }, output())
    await hooks.event!({
      event: {
        type: "message.part.updated",
        properties: {
          part: {
            type: "text",
            sessionID: "session-1",
            text: "Completion evidence:\n\ngoal:complete",
          },
        },
      } as Event,
    })
    await hooks.event!({
      event: {
        type: "session.idle",
        properties: { sessionID: "session-1" },
      } as Event,
    })

    expect(prompted).toHaveLength(1)
    expect(prompted[0]?.body.parts[0]?.text).toContain("prove it")
  })
})
