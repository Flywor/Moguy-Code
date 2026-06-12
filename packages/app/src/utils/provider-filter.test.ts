import { describe, expect, test } from "bun:test"
import { isChatPaidProvider, isChatSelectableProvider } from "./provider-filter"

describe("isChatSelectableProvider", () => {
  test("allows providers connected from environment variables", () => {
    expect(isChatSelectableProvider({ source: "env" })).toBe(true)
  })

  test("allows providers explicitly configured in the app or config files", () => {
    expect(isChatSelectableProvider({ source: "api" })).toBe(true)
    expect(isChatSelectableProvider({ source: "config" })).toBe(true)
    expect(isChatSelectableProvider({ source: "custom" })).toBe(true)
  })

  test("hides the built-in public OpenCode provider", () => {
    expect(isChatSelectableProvider({ id: "opencode", source: "custom", options: { apiKey: "public" } })).toBe(false)
  })

  test("allows OpenCode when it has an explicit non-public key", () => {
    expect(isChatSelectableProvider({ id: "opencode", source: "custom", options: { apiKey: "sk-test" } })).toBe(true)
  })

  test("treats paid environment providers as paid chat providers", () => {
    expect(
      isChatPaidProvider({
        id: "openai",
        source: "env",
        models: { "gpt-5": { cost: { input: 1 } } },
      }),
    ).toBe(true)
  })

  test("keeps configured paid providers and excludes free-only opencode", () => {
    expect(
      isChatPaidProvider({
        id: "deepseek",
        source: "config",
        models: { "deepseek-chat": { cost: { input: 0.1 } } },
      }),
    ).toBe(true)

    expect(
      isChatPaidProvider({
        id: "opencode",
        source: "api",
        models: { "free": { cost: { input: 0 } } },
      }),
    ).toBe(false)

    expect(
      isChatPaidProvider({
        id: "opencode",
        source: "custom",
        options: { apiKey: "public" },
        models: { "paid": { cost: { input: 1 } } },
      }),
    ).toBe(false)
  })
})
