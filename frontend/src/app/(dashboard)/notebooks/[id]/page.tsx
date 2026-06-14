'use client'

import { useState, useEffect } from 'react'
import { useParams } from 'next/navigation'
import { AppShell } from '@/components/layout/AppShell'
import { NotebookHeader } from '../components/NotebookHeader'
import { SourcesColumn } from '../components/SourcesColumn'
import { ChatColumn } from '../components/ChatColumn'
import { CreationsColumn } from '../components/CreationsColumn'
import { useNotebook } from '@/lib/hooks/use-notebooks'
import { useNotebookSources } from '@/lib/hooks/use-sources'
import { useNotes } from '@/lib/hooks/use-notes'
import { LoadingSpinner } from '@/components/common/LoadingSpinner'
import { useNotebookColumnsStore } from '@/lib/stores/notebook-columns-store'
import { useIsWideDesktop } from '@/lib/hooks/use-media-query'
import { useTranslation } from '@/lib/hooks/use-translation'
import { cn } from '@/lib/utils'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { FileText, MessageSquare, Sparkles } from 'lucide-react'

export type ContextMode = 'off' | 'insights' | 'full'

export interface ContextSelections {
  sources: Record<string, ContextMode>
  notes: Record<string, ContextMode>
}

export default function NotebookPage() {
  const { t } = useTranslation()
  const params = useParams()

  // Ensure the notebook ID is properly decoded from URL
  const notebookId = params?.id ? decodeURIComponent(params.id as string) : ''

  const { data: notebook, isLoading: notebookLoading } = useNotebook(notebookId)
  const {
    sources,
    isLoading: sourcesLoading,
    refetch: refetchSources,
    hasNextPage,
    isFetchingNextPage,
    fetchNextPage,
  } = useNotebookSources(notebookId)
  const { data: notes, isLoading: notesLoading } = useNotes(notebookId)

  // Get collapse states for dynamic layout
  const { sourcesCollapsed, creationsCollapsed } = useNotebookColumnsStore()

  // Detect a wide (xl) viewport to avoid double-mounting ChatColumn and to gate
  // the 3-column layout; below xl we fall back to the tabbed layout so chat
  // isn't squished between sources and creations.
  const isDesktop = useIsWideDesktop()

  // Mobile tab state (Sources, Chat, or Creations)
  const [mobileActiveTab, setMobileActiveTab] = useState<'sources' | 'chat' | 'creations'>('chat')

  // Context selection state
  const [contextSelections, setContextSelections] = useState<ContextSelections>({
    sources: {},
    notes: {}
  })

  // Initialize and update selections when sources load or change
  useEffect(() => {
    if (sources && sources.length > 0) {
      setContextSelections(prev => {
        const newSourceSelections = { ...prev.sources }
        sources.forEach(source => {
          const currentMode = newSourceSelections[source.id]
          const hasInsights = source.insights_count > 0

          if (currentMode === undefined) {
            // Initial setup - default based on insights availability
            newSourceSelections[source.id] = hasInsights ? 'insights' : 'full'
          } else if (currentMode === 'full' && hasInsights) {
            // Source gained insights while in 'full' mode - auto-switch to 'insights'
            newSourceSelections[source.id] = 'insights'
          }
        })
        return { ...prev, sources: newSourceSelections }
      })
    }
  }, [sources])

  useEffect(() => {
    if (notes && notes.length > 0) {
      setContextSelections(prev => {
        const newNoteSelections = { ...prev.notes }
        notes.forEach(note => {
          // Only set default if not already set
          if (!(note.id in newNoteSelections)) {
            // Notes default to 'full'
            newNoteSelections[note.id] = 'full'
          }
        })
        return { ...prev, notes: newNoteSelections }
      })
    }
  }, [notes])

  // Handler to update context selection
  const handleContextModeChange = (itemId: string, mode: ContextMode, type: 'source' | 'note') => {
    setContextSelections(prev => ({
      ...prev,
      [type === 'source' ? 'sources' : 'notes']: {
        ...(type === 'source' ? prev.sources : prev.notes),
        [itemId]: mode
      }
    }))
  }

  if (notebookLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    )
  }

  if (!notebook) {
    return (
      <AppShell>
        <div className="p-6">
          <h1 className="text-2xl font-bold mb-4">{t('notebooks.notFound')}</h1>
          <p className="text-muted-foreground">{t('notebooks.notFoundDesc')}</p>
        </div>
      </AppShell>
    )
  }

  const sourcesColumn = (
    <SourcesColumn
      sources={sources}
      isLoading={sourcesLoading}
      notebookId={notebookId}
      notebookName={notebook?.name}
      onRefresh={refetchSources}
      contextSelections={contextSelections.sources}
      onContextModeChange={(sourceId, mode) => handleContextModeChange(sourceId, mode, 'source')}
      notes={notes}
      notesLoading={notesLoading}
      noteContextSelections={contextSelections.notes}
      onNoteContextModeChange={(noteId, mode) => handleContextModeChange(noteId, mode, 'note')}
      hasNextPage={hasNextPage}
      isFetchingNextPage={isFetchingNextPage}
      fetchNextPage={fetchNextPage}
    />
  )

  return (
    <AppShell>
      <div className="flex flex-col flex-1 min-h-0">
        <div className="flex-shrink-0 p-6 pb-0">
          <NotebookHeader notebook={notebook} />
        </div>

        <div className="flex-1 p-6 pt-6 overflow-x-auto flex flex-col">
          {/* Mobile: Tabbed interface - only render on mobile to avoid double-mounting */}
          {!isDesktop && (
            <>
              <div className="xl:hidden mb-4">
                <Tabs value={mobileActiveTab} onValueChange={(value) => setMobileActiveTab(value as 'sources' | 'chat' | 'creations')}>
                  <TabsList className="grid w-full grid-cols-3">
                    <TabsTrigger value="sources" className="gap-2">
                      <FileText className="h-4 w-4" />
                      {t('navigation.sources')}
                    </TabsTrigger>
                    <TabsTrigger value="chat" className="gap-2">
                      <MessageSquare className="h-4 w-4" />
                      {t('common.chat')}
                    </TabsTrigger>
                    <TabsTrigger value="creations" className="gap-2">
                      <Sparkles className="h-4 w-4" />
                      {t('creations.title')}
                    </TabsTrigger>
                  </TabsList>
                </Tabs>
              </div>

              {/* Mobile: Show only active tab */}
              <div className="flex-1 overflow-hidden xl:hidden">
                {mobileActiveTab === 'sources' && sourcesColumn}
                {mobileActiveTab === 'chat' && (
                  <ChatColumn
                    notebookId={notebookId}
                    contextSelections={contextSelections}
                    sources={sources}
                    sourcesLoading={sourcesLoading}
                  />
                )}
                {mobileActiveTab === 'creations' && (
                  <CreationsColumn notebookId={notebookId} />
                )}
              </div>
            </>
          )}

          {/* Desktop: Collapsible columns layout */}
          <div className={cn(
            'hidden xl:flex h-full min-h-0 gap-6 transition-all duration-150',
            'flex-row'
          )}>
            {/* Sources Column (sources + notes) */}
            <div className={cn(
              'transition-all duration-150',
              sourcesCollapsed ? 'w-12 flex-shrink-0' : 'flex-none basis-1/3 min-w-0'
            )}>
              {sourcesColumn}
            </div>

            {/* Chat Column - always expanded, takes remaining space */}
            <div className="transition-all duration-150 flex-1 min-w-0">
              <ChatColumn
                notebookId={notebookId}
                contextSelections={contextSelections}
                sources={sources}
                sourcesLoading={sourcesLoading}
              />
            </div>

            {/* Creations Column */}
            <div className={cn(
              'transition-all duration-150 lg:pr-6 lg:-mr-6',
              creationsCollapsed ? 'w-12 flex-shrink-0' : 'flex-none basis-1/3 min-w-0'
            )}>
              <CreationsColumn notebookId={notebookId} />
            </div>
          </div>
        </div>
      </div>
    </AppShell>
  )
}
