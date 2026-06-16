export function useUpdaterAction() {
  return {
    action: () => ({ label: "", run: false }),
    run: () => {},
  }
}
