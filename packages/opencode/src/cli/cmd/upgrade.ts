import type { Argv } from "yargs"
import { UI } from "../ui"
import * as prompts from "@clack/prompts"

export const UpgradeCommand = {
  command: "upgrade [target]",
  describe: "upgrade opencode to the latest or a specific version",
  handler: async () => {
    UI.empty()
    UI.println(UI.logo("  "))
    UI.empty()
    prompts.log.warn("Auto-upgrade has been disabled in this build.")
    prompts.outro("Done")
  },
}
