'use client'

import { useMemo, useState } from 'react'
import { useParams } from 'next/navigation'
import { Plus, Loader2 } from 'lucide-react'

import { AppShell } from '@/components/layout/AppShell'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useTranslation } from '@/lib/hooks/use-translation'
import { useNotebooks } from '@/lib/hooks/use-notebooks'
import { useCreators, useCreationArtifacts } from '@/lib/hooks/use-creation'
import { GenerateArtifactDialog } from '@/components/creation/GenerateArtifactDialog'
import { CreationArtifactCard } from '@/components/creation/CreationArtifactCard'

const ALL = '__all__'

export default function CreationPage() {
  const { t } = useTranslation()
  const params = useParams<{ key: string }>()
  const creatorKey = params.key

  const { creators, isLoading: creatorsLoading } = useCreators()
  const manifest = useMemo(
    () => creators.find(c => c.key === creatorKey),
    [creators, creatorKey]
  )

  const { data: notebooks } = useNotebooks()
  const [notebookFilter, setNotebookFilter] = useState<string>(ALL)
  const [dialogOpen, setDialogOpen] = useState(false)

  const notebookId = notebookFilter === ALL ? undefined : notebookFilter
  const { artifacts, isLoading } = useCreationArtifacts(creatorKey, notebookId)

  return (
    <AppShell>
      <div className="flex-1 overflow-y-auto">
        <div className="px-6 py-6 space-y-6">
          <div className="flex items-start justify-between gap-4">
            <header className="space-y-1">
              <h1 className="text-2xl font-semibold tracking-tight">
                {manifest?.name ?? creatorKey}
              </h1>
              <p className="text-muted-foreground">
                {manifest?.description ?? t('creation.listDesc')}
              </p>
            </header>
            <Button onClick={() => setDialogOpen(true)} disabled={!manifest?.available}>
              <Plus className="h-4 w-4" />
              {t('creation.generate')}
            </Button>
          </div>

          <div className="w-64">
            <Select value={notebookFilter} onValueChange={setNotebookFilter}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL}>{t('creation.allNotebooks')}</SelectItem>
                {(notebooks ?? []).map(nb => (
                  <SelectItem key={nb.id} value={nb.id}>
                    {nb.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {creatorsLoading || isLoading ? (
            <div className="flex items-center gap-2 py-10 text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              {t('common.loading')}
            </div>
          ) : !manifest ? (
            <p className="text-muted-foreground">{t('creation.creatorNotFound')}</p>
          ) : artifacts.length === 0 ? (
            <p className="py-10 text-center text-muted-foreground">
              {t('creation.noArtifactsYet')}
            </p>
          ) : (
            <div className="grid gap-4">
              {artifacts.map(artifact => (
                <CreationArtifactCard
                  key={artifact.id}
                  artifact={artifact}
                  notebookId={notebookId}
                />
              ))}
            </div>
          )}
        </div>
      </div>

      {manifest && (
        <GenerateArtifactDialog
          manifest={manifest}
          notebookId={notebookId}
          open={dialogOpen}
          onOpenChange={setDialogOpen}
        />
      )}
    </AppShell>
  )
}
