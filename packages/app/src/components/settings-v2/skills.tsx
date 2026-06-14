import { type SkillV2Info } from "@opencode-ai/sdk/v2/client"
import { useFilteredList } from "@opencode-ai/ui/hooks"
import { Icon as IconV2 } from "@opencode-ai/ui/v2/icon"
import { IconButtonV2 } from "@opencode-ai/ui/v2/icon-button-v2"
import { TextInputV2 } from "@opencode-ai/ui/v2/text-input-v2"
import { useParams } from "@solidjs/router"
import { type Component, For, Show, createMemo, createResource, createSignal } from "solid-js"
import { useLanguage } from "@/context/language"
import { useServerSDK } from "@/context/server-sdk"
import { decode64 } from "@/utils/base64"
import { SettingsListV2 } from "./parts/list"
import "./settings-v2.css"

type SkillSource = "claude" | "agents" | "opencode" | "custom"

function sourceOf(location: string): SkillSource {
  const normalized = location.replace(/\\/g, "/")
  if (normalized.includes("/.claude/skills/")) return "claude"
  if (normalized.includes("/.agents/skills/")) return "agents"
  if (normalized.includes("/.opencode/skill/") || normalized.includes("/.opencode/skills/")) return "opencode"
  return "custom"
}

function directoryOf(location: string) {
  const normalized = location.replace(/\\/g, "/")
  const index = normalized.lastIndexOf("/")
  return index === -1 ? location : location.slice(0, index)
}

export const SettingsSkillsV2: Component = () => {
  const language = useLanguage()
  const params = useParams()
  const serverSdk = useServerSDK()
  const [loadFailed, setLoadFailed] = createSignal(false)
  const location = createMemo(() => {
    const directory = decode64(params.dir)
    return { directory }
  })

  const [skills] = createResource(
    location,
    async (location) => {
      setLoadFailed(false)
      try {
        const response = await serverSdk().client.v2.skill.list(
          { location: location.directory ? { directory: location.directory } : undefined },
          { throwOnError: true },
        )
        return response.data.data
      } catch (error) {
        console.error("[settings-skills] failed to load skills", error)
        setLoadFailed(true)
        return [] as SkillV2Info[]
      }
    },
    { initialValue: [] as SkillV2Info[] },
  )

  const list = useFilteredList<SkillV2Info>({
    items: () => skills.latest ?? [],
    key: (skill) => skill.name,
    filterKeys: ["name", "description", "location"],
    sortBy: (a, b) => a.name.localeCompare(b.name),
    groupBy: (skill) => sourceOf(skill.location),
    sortGroupsBy: (a, b) => {
      const order: SkillSource[] = ["claude", "agents", "opencode", "custom"]
      return order.indexOf(a.category as SkillSource) - order.indexOf(b.category as SkillSource)
    },
  })

  const sourceLabel = (source: string) => {
    switch (source) {
      case "claude":
        return language.t("settings.skills.source.claude")
      case "agents":
        return language.t("settings.skills.source.agents")
      case "opencode":
        return language.t("settings.skills.source.opencode")
      default:
        return language.t("settings.skills.source.custom")
    }
  }

  return (
    <>
      <div class="settings-v2-tab-header settings-v2-tab-header--stacked">
        <div class="settings-v2-tab-header-row">
          <h2 class="settings-v2-tab-title">{language.t("settings.skills.title")}</h2>
          <div class="settings-v2-skills-count">{language.t("settings.skills.count", { count: skills.latest.length })}</div>
        </div>
        <div class="settings-v2-tab-search">
          <TextInputV2
            type="search"
            appearance="base"
            value={list.filter()}
            onInput={(event) => list.onInput(event.currentTarget.value)}
            placeholder={language.t("settings.skills.search.placeholder")}
            spellcheck={false}
            autocorrect="off"
            autocomplete="off"
            autocapitalize="off"
            aria-label={language.t("settings.skills.search.placeholder")}
          />
          <Show when={list.filter()}>
            <IconButtonV2
              type="button"
              variant="ghost-muted"
              size="small"
              class="settings-v2-tab-search-clear"
              icon={<IconV2 name="close" size="large" class="text-v2-icon-icon-muted" />}
              onClick={() => list.clear()}
            />
          </Show>
        </div>
      </div>

      <div class="settings-v2-tab-body settings-v2-skills">
        <Show
          when={!skills.loading}
          fallback={
            <div class="settings-v2-skills-status">
              {language.t("common.loading")}
              {language.t("common.loading.ellipsis")}
            </div>
          }
        >
          <Show
            when={!loadFailed()}
            fallback={<div class="settings-v2-skills-status">{language.t("settings.skills.error")}</div>}
          >
            <Show
              when={list.flat().length > 0}
              fallback={
                <div class="settings-v2-skills-status">
                  <span>{language.t("settings.skills.empty")}</span>
                  <Show when={list.filter()}>
                    <span class="settings-v2-skills-status-filter">&quot;{list.filter()}&quot;</span>
                  </Show>
                </div>
              }
            >
              <For each={list.grouped.latest}>
                {(group) => (
                  <div class="settings-v2-section" data-component="settings-skills-source">
                    <h3 class="settings-v2-section-title">{sourceLabel(group.category)}</h3>
                    <SettingsListV2>
                      <For each={group.items}>
                        {(skill) => (
                          <div class="settings-v2-skills-row">
                            <div class="settings-v2-skills-copy">
                              <div class="settings-v2-skills-main">
                                <span class="settings-v2-skills-name">{skill.name}</span>
                                <Show when={skill.slash}>
                                  <span class="settings-v2-skills-badge">{language.t("settings.skills.slash")}</span>
                                </Show>
                              </div>
                              <Show when={skill.description}>
                                {(description) => <p class="settings-v2-skills-description">{description()}</p>}
                              </Show>
                              <div class="settings-v2-skills-location">{directoryOf(skill.location)}</div>
                            </div>
                          </div>
                        )}
                      </For>
                    </SettingsListV2>
                  </div>
                )}
              </For>
            </Show>
          </Show>
        </Show>
      </div>
    </>
  )
}
