'use client'

import { useCallback, useMemo, useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Sparkles, Mic, Plus } from 'lucide-react'
import { LoadingSpinner } from '@/components/common/LoadingSpinner'
import { EmptyState } from '@/components/common/EmptyState'
import { CollapsibleColumn, createCollapseButton } from '@/components/notebooks/CollapsibleColumn'
import { useNotebookColumnsStore } from '@/lib/stores/notebook-columns-store'
import { useTranslation } from '@/lib/hooks/use-translation'
import { useCreators, useCreationArtifacts } from '@/lib/hooks/use-creation'
import { CreatorManifest } from '@/lib/types/creation'
import { GenerateArtifactDialog } from '@/components/creation/GenerateArtifactDialog'
import { CreationArtifactCard } from '@/components/creation/CreationArtifactCard'
import { GeneratePodcastDialog } from '@/components/podcasts/GeneratePodcastDialog'
import { EpisodeCard } from '@/components/podcasts/EpisodeCard'
import {
  usePodcastEpisodes,
  useDeletePodcastEpisode,
  useRetryPodcastEpisode,
} from '@/lib/hooks/use-podcasts'

interface CreationsColumnProps {
  notebookId: string
}

/** A shortcut tile that opens a creator's generation dialog pre-scoped to the notebook. */
function CreatorShortcut({
  manifest,
  notebookId,
}: {
  manifest: CreatorManifest
  notebookId: string
}) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <Button
        variant="outline"
        className="h-auto flex-col items-start gap-1 p-3 text-left"
        disabled={!manifest.available}
        onClick={() => setOpen(true)}
      >
        <Sparkles className="h-4 w-4 text-primary" />
        <span className="text-sm font-medium">{manifest.name}</span>
      </Button>
      <GenerateArtifactDialog
        manifest={manifest}
        notebookId={notebookId}
        open={open}
        onOpenChange={setOpen}
      />
    </>
  )
}

/** A shortcut tile for podcast generation, scoped to the notebook. */
function PodcastShortcut({ notebookId }: { notebookId: string }) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)
  return (
    <>
      <Button
        variant="outline"
        className="h-auto flex-col items-start gap-1 p-3 text-left"
        onClick={() => setOpen(true)}
      >
        <Mic className="h-4 w-4 text-primary" />
        <span className="text-sm font-medium">{t('creations.podcast')}</span>
      </Button>
      <GeneratePodcastDialog open={open} onOpenChange={setOpen} defaultNotebookId={notebookId} />
    </>
  )
}

/** Lists existing artifacts for a single creator, scoped to the notebook. */
function CreatorArtifactList({
  creatorKey,
  notebookId,
}: {
  creatorKey: string
  notebookId: string
}) {
  const { artifacts } = useCreationArtifacts(creatorKey, notebookId)
  if (artifacts.length === 0) return null
  return (
    <>
      {artifacts.map((artifact) => (
        <CreationArtifactCard key={artifact.id} artifact={artifact} notebookId={notebookId} />
      ))}
    </>
  )
}

/** Lists podcast episodes scoped to the notebook. */
function PodcastEpisodeList({ notebookId }: { notebookId: string }) {
  const { episodes } = usePodcastEpisodes({ notebookId })
  const deleteEpisode = useDeletePodcastEpisode()
  const retryEpisode = useRetryPodcastEpisode()

  const handleDelete = useCallback(
    (episodeId: string) => deleteEpisode.mutateAsync(episodeId),
    [deleteEpisode]
  )
  const handleRetry = useCallback(
    async (episodeId: string) => {
      await retryEpisode.mutateAsync(episodeId)
    },
    [retryEpisode]
  )

  if (episodes.length === 0) return null
  return (
    <>
      {episodes.map((episode) => (
        <EpisodeCard
          key={episode.id}
          episode={episode}
          onDelete={handleDelete}
          deleting={deleteEpisode.isPending}
          onRetry={handleRetry}
          retrying={retryEpisode.isPending}
        />
      ))}
    </>
  )
}

export function CreationsColumn({ notebookId }: CreationsColumnProps) {
  const { t } = useTranslation()
  const { creators, isLoading: creatorsLoading } = useCreators()

  const { creationsCollapsed, toggleCreations } = useNotebookColumnsStore()
  const collapseButton = useMemo(
    () => createCollapseButton(toggleCreations, t('creations.title')),
    [toggleCreations, t('creations.title')]
  )

  // Episodes for empty-state detection (also drives the list section)
  const { episodes } = usePodcastEpisodes({ notebookId })

  const availableCreators = useMemo(
    () => creators.filter((c) => c.available),
    [creators]
  )

  return (
    <CollapsibleColumn
      isCollapsed={creationsCollapsed}
      onToggle={toggleCreations}
      collapsedIcon={Sparkles}
      collapsedLabel={t('creations.title')}
    >
      <Card className="h-full flex flex-col flex-1 overflow-hidden">
        <CardHeader className="pb-3 flex-shrink-0">
          <div className="flex items-center justify-between gap-2">
            <CardTitle className="text-lg">{t('creations.title')}</CardTitle>
            {collapseButton}
          </div>
        </CardHeader>

        <CardContent className="flex-1 overflow-y-auto min-h-0 space-y-4">
          {/* Shortcuts to create flows */}
          <div className="grid grid-cols-2 gap-2">
            {creatorsLoading ? (
              <div className="col-span-2 flex items-center justify-center py-4">
                <LoadingSpinner />
              </div>
            ) : (
              <>
                {availableCreators.map((manifest) => (
                  <CreatorShortcut
                    key={manifest.key}
                    manifest={manifest}
                    notebookId={notebookId}
                  />
                ))}
                <PodcastShortcut notebookId={notebookId} />
              </>
            )}
          </div>

          {/* Existing creations */}
          <div className="space-y-3">
            {availableCreators.map((manifest) => (
              <CreatorArtifactList
                key={manifest.key}
                creatorKey={manifest.key}
                notebookId={notebookId}
              />
            ))}
            <PodcastEpisodeList notebookId={notebookId} />

            {!creatorsLoading && episodes.length === 0 && availableCreators.length === 0 && (
              <EmptyState
                icon={Plus}
                title={t('creations.noCreationsYet')}
                description={t('creations.noCreationsDesc')}
              />
            )}
          </div>
        </CardContent>
      </Card>
    </CollapsibleColumn>
  )
}
