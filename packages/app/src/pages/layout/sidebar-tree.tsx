import { For, Show, createMemo, createSignal, onCleanup, type Accessor, type JSX } from "solid-js"
import { useNavigate, useParams } from "@solidjs/router"
import { Collapsible } from "@opencode-ai/ui/collapsible"
import { DropdownMenu } from "@opencode-ai/ui/dropdown-menu"
import { Icon } from "@opencode-ai/ui/icon"
import { IconButton } from "@opencode-ai/ui/icon-button"
import { InlineInput } from "@opencode-ai/ui/inline-input"
import { Tooltip, TooltipKeybind } from "@opencode-ai/ui/tooltip"
import { Spinner } from "@opencode-ai/ui/spinner"
import { base64Encode } from "@opencode-ai/core/util/encode"
import { getFilename } from "@opencode-ai/core/util/path"
import { useServerSync, useQueryOptions } from "@/context/server-sync"
import { useLanguage } from "@/context/language"
import { useLayout, type LocalProject } from "@/context/layout"
import { useCommand } from "@/context/command"
import { useNotification } from "@/context/notification"
import { usePermission } from "@/context/permission"
import { useServerSDK } from "@/context/server-sdk"
import { showToast } from "@/utils/toast"
import { pathKey } from "@/utils/path-key"
import { sessionTitle } from "@/utils/session-title"
import { messageAgentColor } from "@/utils/agent"
import { sessionPermissionRequest } from "../session/composer/session-request-tree"
import { sortedRootSessions } from "./helpers"
import { useIsFetching } from "@tanstack/solid-query"
import type { Session, PermissionRequest } from "@opencode-ai/sdk/v2/client"
import { Switch, Match } from "solid-js"

export function SidebarTree(props: {
  mobile?: boolean
  openProjectLabel: JSX.Element
  openProjectKeybind: Accessor<string | undefined>
  onOpenProject: () => void
  settingsLabel: Accessor<string>
  settingsKeybind: Accessor<string | undefined>
  onOpenSettings: () => void
  helpLabel: Accessor<string>
  onOpenHelp: () => void
}): JSX.Element {
  const layout = useLayout()
  const command = useCommand()
  const projects = createMemo(() => layout.projects.list())
  const placement = () => (props.mobile ? "bottom" : "right")

  return (
    <div class="flex flex-col h-full min-w-0">
      <div class="flex-1 min-h-0 overflow-y-auto no-scrollbar py-3">
        <For each={projects()}>
          {(project) => <ProjectSection project={project} mobile={props.mobile} />}
        </For>
      </div>
      <div class="shrink-0 px-3 pb-3 flex items-center gap-1">
        <Tooltip
          placement={placement()}
          value={
            <div class="flex items-center gap-2">
              <span>{props.openProjectLabel}</span>
              <Show when={!props.mobile && !!props.openProjectKeybind()}>
                <span class="text-icon-base text-12-medium">{props.openProjectKeybind()}</span>
              </Show>
            </div>
          }
        >
          <IconButton
            icon="plus"
            variant="ghost"
            size="large"
            onClick={props.onOpenProject}
            aria-label={typeof props.openProjectLabel === "string" ? props.openProjectLabel : undefined}
          />
        </Tooltip>
        <TooltipKeybind placement={placement()} title={props.settingsLabel()} keybind={props.settingsKeybind() ?? ""}>
          <IconButton
            icon="settings-gear"
            variant="ghost"
            size="large"
            onClick={props.onOpenSettings}
            aria-label={props.settingsLabel()}
          />
        </TooltipKeybind>
        <TooltipKeybind placement={placement()} title={props.helpLabel()} keybind={command.keybind("help") ?? ""}>
          <IconButton
            icon="help"
            variant="ghost"
            size="large"
            onClick={props.onOpenHelp}
            aria-label={props.helpLabel()}
          />
        </TooltipKeybind>
      </div>
    </div>
  )
}

function ProjectSection(props: {
  project: LocalProject
  mobile?: boolean
}): JSX.Element {
  const language = useLanguage()
  const navigate = useNavigate()
  const serverSync = useServerSync()
  const queryOptions = useQueryOptions()
  const slug = createMemo(() => base64Encode(props.project.worktree))
  const name = createMemo(() => props.project.name || getFilename(props.project.worktree))
  const [store] = serverSync.child(props.project.worktree)
  const sortNow = createMemo(() => Date.now())
  const sessions = createMemo(() => sortedRootSessions(store, sortNow()))
  const count = createMemo(() => sessions()?.length ?? 0)
  const fetching = useIsFetching(() => queryOptions.sessions(pathKey(props.project.worktree)))
  const loading = () => fetching() > 0 && count() === 0

  const [expanded, setExpanded] = createSignal(true)

  return (
    <Collapsible variant="ghost" open={expanded()} onOpenChange={setExpanded} class="shrink-0">
      <Collapsible.Trigger class="flex items-center justify-between w-full px-3 py-1.5 text-left hover:bg-surface-raised-base-hover rounded-md group">
        <div class="flex items-center gap-2 min-w-0 flex-1"
          onClick={(e) => e.stopPropagation()}
        >
          <Icon
            name="chevron-right"
            size="small"
            class="transition-transform duration-150 group-data-[expanded]:rotate-90"
          />
          <Icon name="folder" size="small" class="text-icon-base shrink-0" />
          <span class="text-14-medium text-text-strong truncate">{name()}</span>
          <span class="text-12-regular text-text-weak shrink-0">{count()}</span>
        </div>
        <button
          type="button"
          class="shrink-0 size-6 flex items-center justify-center rounded-md opacity-0 group-hover:opacity-100 hover:bg-surface-raised-base-hover transition-opacity"
          onClick={(e) => {
            e.stopPropagation()
            const dir = props.project.worktree
            if (!dir) return
            navigate(`/${base64Encode(dir)}/session`)
          }}
          title={language.t("command.session.new")}
        >
          <Icon name="plus" size="small" />
        </button>
      </Collapsible.Trigger>
      <Collapsible.Content>
        <div class="pl-10 pr-2 py-1">
          <Show when={!loading()} fallback={<Spinner class="size-4 ml-2 my-2" />}>
            <For each={sessions()}>
              {(session) => <TreeSessionRow session={session} slug={slug()} mobile={props.mobile} />}
            </For>
          </Show>
        </div>
      </Collapsible.Content>
    </Collapsible>
  )
}

function TreeSessionRow(props: {
  session: Session
  slug: string
  mobile?: boolean
}): JSX.Element {
  const notification = useNotification()
  const permission = usePermission()
  const serverSync = useServerSync()
  const params = useParams()
  const language = useLanguage()
  const sdk = useServerSDK()

  const title = createMemo(() => sessionTitle(props.session.title) || props.session.id)
  const unseenCount = createMemo(() => notification.session.unseenCount(props.session.id))
  const hasError = createMemo(() => notification.session.unseenHasError(props.session.id))
  const [sessionStore, setSessionStore] = serverSync.child(props.session.directory)
  const hasPermissions = createMemo(() => {
    return !!sessionPermissionRequest(sessionStore.session, sessionStore.permission, props.session.id, (item: PermissionRequest) => {
      return !permission.autoResponds(item, props.session.directory)
    })
  })
  const isWorking = createMemo(() => {
    if (hasPermissions()) return false
    return sessionStore.session_working(props.session.id)
  })
  const tint = createMemo(() => messageAgentColor(sessionStore.message[props.session.id], sessionStore.agent))
  const isActive = createMemo(() => params.id === props.session.id)
  const [renaming, setRenaming] = createSignal(false)
  const [draft, setDraft] = createSignal("")
  let renameInput: HTMLInputElement | undefined
  let renameFrame: number | undefined

  onCleanup(() => {
    if (renameFrame !== undefined) cancelAnimationFrame(renameFrame)
  })

  const openRename = () => {
    setDraft(title())
    setRenaming(true)
    if (renameFrame !== undefined) cancelAnimationFrame(renameFrame)
    renameFrame = requestAnimationFrame(() => {
      renameFrame = undefined
      renameInput?.focus()
      renameInput?.select()
    })
  }
  const saveRename = async () => {
    if (!renaming()) return
    setRenaming(false)
    const next = draft().trim()
    if (!next || next === title()) return
    try {
      await sdk.client.session.update({ sessionID: props.session.id, title: next })
      setSessionStore("session", (items) =>
        items.map((item) => (item.id === props.session.id ? { ...item, title: next } : item)),
      )
    } catch (err) {
      showToast({
        title: language.t("common.requestFailed"),
        description: err instanceof Error ? err.message : String(err),
      })
    }
  }
  const archiveSession = async () => {
    try {
      await sdk.client.session.update({ sessionID: props.session.id, time: { archived: Date.now() } })
    } catch {
      showToast({ title: language.t("common.requestFailed"), description: "" })
    }
  }
  const deleteSession = async () => {
    if (!window.confirm(language.t("session.delete.confirm", { name: title() }))) return
    try {
      await sdk.client.session.delete({ sessionID: props.session.id })
    } catch {
      showToast({ title: language.t("session.delete.failed.title"), description: "" })
    }
  }

  return (
    <div class="group/session flex items-center min-w-0 w-full rounded-md transition-colors hover:bg-surface-raised-base-hover"
      classList={{
        "bg-surface-base-active": isActive(),
      }}
    >
      <Show
        when={renaming()}
        fallback={
          <a
            href={`/${props.slug}/session/${props.session.id}`}
            class="flex items-center gap-2 min-w-0 flex-1 px-2 py-1 text-left focus:outline-none"
          >
            <span class="text-13-regular text-text-strong min-w-0 flex-1 truncate">{title()}</span>
            <Show
              when={isWorking() || hasPermissions() || hasError() || unseenCount() > 0}
              fallback={<div class="size-4 shrink-0" />}
            >
              <div
                class="shrink-0 size-4 flex items-center justify-center"
                style={{ color: tint() ?? "var(--icon-interactive-base)" }}
              >
                <Switch>
                  <Match when={isWorking()}>
                    <Spinner class="size-3" />
                  </Match>
                  <Match when={hasPermissions()}>
                    <div class="size-1.5 rounded-full bg-surface-warning-strong" />
                  </Match>
                  <Match when={hasError()}>
                    <div class="size-1.5 rounded-full bg-text-diff-delete-base" />
                  </Match>
                  <Match when={unseenCount() > 0}>
                    <div class="size-1.5 rounded-full bg-text-interactive-base" />
                  </Match>
                </Switch>
              </div>
            </Show>
          </a>
        }
      >
        <div class="flex min-w-0 flex-1 px-2 py-1">
          <InlineInput
            ref={(el) => {
              renameInput = el
            }}
            value={draft()}
            class="min-w-0 flex-1 text-13-regular text-text-strong"
            onInput={(event) => setDraft(event.currentTarget.value)}
            onBlur={() => void saveRename()}
            onKeyDown={(event) => {
              event.stopPropagation()
              if (event.key === "Enter") {
                event.preventDefault()
                void saveRename()
                return
              }
              if (event.key === "Escape") {
                event.preventDefault()
                setRenaming(false)
              }
            }}
            onPointerDown={(event) => event.stopPropagation()}
            onClick={(event) => event.stopPropagation()}
          />
        </div>
      </Show>
      <DropdownMenu gutter={4} placement="bottom-end">
        <DropdownMenu.Trigger
          as={IconButton}
          icon="dot-grid"
          variant="ghost"
          size="small"
          class="shrink-0 opacity-0 group-hover/session:opacity-100 mr-1 data-[expanded]:opacity-100"
          aria-label={language.t("common.moreOptions")}
        />
        <DropdownMenu.Portal>
          <DropdownMenu.Content style={{ "min-width": "104px" }}>
            <DropdownMenu.Item onSelect={() => openRename()}>
              <DropdownMenu.ItemLabel>{language.t("common.rename")}</DropdownMenu.ItemLabel>
            </DropdownMenu.Item>
            <DropdownMenu.Item onSelect={() => void archiveSession()}>
              <DropdownMenu.ItemLabel>{language.t("common.archive")}</DropdownMenu.ItemLabel>
            </DropdownMenu.Item>
            <DropdownMenu.Separator />
            <DropdownMenu.Item onSelect={() => void deleteSession()}>
              <DropdownMenu.ItemLabel>{language.t("common.delete")}</DropdownMenu.ItemLabel>
            </DropdownMenu.Item>
          </DropdownMenu.Content>
        </DropdownMenu.Portal>
      </DropdownMenu>
    </div>
  )
}
