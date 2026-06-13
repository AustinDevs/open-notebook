import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface NotebookColumnsState {
  sourcesCollapsed: boolean
  creationsCollapsed: boolean
  toggleSources: () => void
  toggleCreations: () => void
  setSources: (collapsed: boolean) => void
  setCreations: (collapsed: boolean) => void
}

export const useNotebookColumnsStore = create<NotebookColumnsState>()(
  persist(
    (set) => ({
      sourcesCollapsed: false,
      creationsCollapsed: false,
      toggleSources: () => set((state) => ({ sourcesCollapsed: !state.sourcesCollapsed })),
      toggleCreations: () => set((state) => ({ creationsCollapsed: !state.creationsCollapsed })),
      setSources: (collapsed) => set({ sourcesCollapsed: collapsed }),
      setCreations: (collapsed) => set({ creationsCollapsed: collapsed }),
    }),
    {
      name: 'notebook-columns-storage-v2',
    }
  )
)
