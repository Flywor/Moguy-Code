import type { UpdaterState } from "@opencode-ai/app/updater"
import { createUpdaterController } from "./updater-controller"

export function setupAutoUpdater(_stop: () => Promise<void>) {
  return createUpdaterController({
    enabled: false,
    currentVersion: "",
    backend: {
      checkForUpdates: () => Promise.resolve(null),
      downloadUpdate: () => Promise.resolve(),
      quitAndInstall: () => {},
    },
    persistence: {
      get: () => undefined,
      set: () => {},
      clear: () => {},
    },
    stop: () => Promise.resolve(),
  })
}

export async function showUpdaterDialog(_controller?: unknown, _alertOnFail?: boolean) {}
