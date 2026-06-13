'use client'

import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { CreationArtifact } from '@/lib/types/creation'

/**
 * Shown when no renderer is registered for an artifact's schema_id (or its data
 * fails validation). Never executes/loads remote code — just shows metadata,
 * files, and the raw JSON for debugging.
 */
export function FallbackRenderer({ artifact }: { artifact: CreationArtifact }) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)

  return (
    <div className="space-y-3 text-sm">
      <p className="text-muted-foreground">
        {t('creation.rendererUnavailable')} (<code>{artifact.schema_id ?? 'unknown'}</code>)
      </p>
      <Button variant="ghost" size="sm" onClick={() => setOpen(o => !o)}>
        {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        {t('creation.viewRawData')}
      </Button>
      {open && (
        <pre className="max-h-96 overflow-auto rounded-md bg-muted p-3 text-xs">
          {JSON.stringify(artifact.data, null, 2)}
        </pre>
      )}
    </div>
  )
}
