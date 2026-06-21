import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface NotebookColumnsState {
  sourcesCollapsed: boolean
  notesCollapsed: boolean
  creationsCollapsed: boolean
  toggleSources: () => void
  toggleNotes: () => void
  toggleCreations: () => void
  setSources: (collapsed: boolean) => void
  setNotes: (collapsed: boolean) => void
  setCreations: (collapsed: boolean) => void
}

export const useNotebookColumnsStore = create<NotebookColumnsState>()(
  persist(
    (set) => ({
      sourcesCollapsed: false,
      notesCollapsed: false,
      creationsCollapsed: false,
      toggleSources: () => set((state) => ({ sourcesCollapsed: !state.sourcesCollapsed })),
      toggleNotes: () => set((state) => ({ notesCollapsed: !state.notesCollapsed })),
      toggleCreations: () => set((state) => ({ creationsCollapsed: !state.creationsCollapsed })),
      setSources: (collapsed) => set({ sourcesCollapsed: collapsed }),
      setNotes: (collapsed) => set({ notesCollapsed: collapsed }),
      setCreations: (collapsed) => set({ creationsCollapsed: collapsed }),
    }),
    {
      name: 'notebook-columns-storage-v2',
    }
  )
)
