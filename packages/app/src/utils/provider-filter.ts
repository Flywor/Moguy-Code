export function isChatSelectableProvider(provider: { id?: string; source?: string; options?: Record<string, unknown> }) {
  if (provider.id === "opencode" && provider.options?.apiKey === "public") return false
  return true
}

export function isChatPaidProvider(provider: {
  id: string
  source?: string
  options?: Record<string, unknown>
  models?: Record<string, { cost?: { input?: number } }>
}) {
  if (!isChatSelectableProvider(provider)) return false
  return provider.id !== "opencode" || Object.values(provider.models ?? {}).some((model) => model.cost?.input)
}
