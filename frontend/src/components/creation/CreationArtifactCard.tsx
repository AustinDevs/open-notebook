'use client'

import { useState } from 'react'
import { Loader2, Trash2, AlertTriangle, ChevronDown, ChevronRight } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { useTranslation } from '@/lib/hooks/use-translation'
import { useCreators, useDeleteCreationArtifact } from '@/lib/hooks/use-creation'
import { CreationArtifact } from '@/lib/types/creation'
import { getRenderer } from './renderers/registry'
import { PluginViewRenderer } from './renderers/PluginViewRenderer'

const STATUS_VARIANT: Record<string, string> = {
  completed: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  partial: 'bg-amber-50 text-amber-800 border-amber-200',
  failed: 'bg-red-50 text-red-700 border-red-200',
  running: 'bg-blue-50 text-blue-700 border-blue-200',
  submitted: 'bg-blue-50 text-blue-700 border-blue-200',
}

// Literal keys (not built dynamically) so i18n usage is statically detectable.
const STATUS_LABEL_KEY: Record<string, string> = {
  submitted: 'creation.status.submitted',
  running: 'creation.status.running',
  completed: 'creation.status.completed',
  partial: 'creation.status.partial',
  failed: 'creation.status.failed',
}

interface Props {
  artifact: CreationArtifact
  notebookId?: string
}

export function CreationArtifactCard({ artifact, notebookId }: Props) {
  const { t } = useTranslation()
  const del = useDeleteCreationArtifact(artifact.creator_key, notebookId)
  // Prefer the plugin's own view: a shipped bundle (`has_view`) or a self-contained
  // HTML file the plugin emitted. Otherwise fall back to a core schema renderer.
  const { creators } = useCreators()
  const hasViewBundle = creators.find(c => c.key === artifact.creator_key)?.has_view
  const hasHtmlFile = (artifact.files ?? []).some(
    f => f.content_type === 'text/html' || f.filename.toLowerCase().endsWith('.html')
  )
  const usePluginView = Boolean(hasViewBundle) || hasHtmlFile
  const Renderer = getRenderer(artifact.schema_id)
  const isActive = artifact.status === 'running' || artifact.status === 'submitted'
  const isDone = artifact.status === 'completed' || artifact.status === 'partial'

  // Generated assets collapse into a card by default and open on click, so the
  // notebook column isn't flooded with full renders.
  const [expanded, setExpanded] = useState(false)
  const Chevron = expanded ? ChevronDown : ChevronRight

  return (
    <div className="rounded-lg border bg-card p-4 space-y-3">
      <div className="flex items-start justify-between gap-3">
        <button
          type="button"
          onClick={() => isDone && setExpanded((v) => !v)}
          disabled={!isDone}
          aria-expanded={isDone ? expanded : undefined}
          className="flex flex-1 items-start gap-2 text-left disabled:cursor-default"
        >
          {isDone && (
            <Chevron className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
          )}
          <div className="space-y-1">
            <h3 className="font-medium">{artifact.name}</h3>
            <Badge
              variant="outline"
              className={STATUS_VARIANT[artifact.status] ?? ''}
            >
              {t(STATUS_LABEL_KEY[artifact.status] ?? 'creation.status.submitted')}
            </Badge>
          </div>
        </button>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => del.mutate(artifact.id)}
          disabled={del.isPending}
          aria-label={t('common.delete')}
        >
          <Trash2 className="h-4 w-4" />
        </Button>
      </div>

      {artifact.warnings.length > 0 && (
        <div className="flex items-start gap-2 rounded-md bg-amber-50 p-2 text-xs text-amber-800">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>{artifact.warnings.join(' ')}</span>
        </div>
      )}

      {isActive && (
        <div className="flex items-center gap-2 py-6 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          {t('creation.generating')}
        </div>
      )}

      {artifact.status === 'failed' && (
        <p className="text-sm text-destructive">
          {artifact.user_message || artifact.error_message || t('creation.failedToGenerate')}
        </p>
      )}

      {isDone &&
        expanded &&
        (usePluginView ? (
          <PluginViewRenderer artifact={artifact} hasViewBundle={hasViewBundle} />
        ) : (
          <Renderer artifact={artifact} />
        ))}
    </div>
  )
}
