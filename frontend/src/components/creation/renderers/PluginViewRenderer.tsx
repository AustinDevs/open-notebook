'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import { Download, ExternalLink } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { creationApi, triggerBrowserDownload } from '@/lib/api/creation'
import { CreationArtifact } from '@/lib/types/creation'

const READY = 'open-notebook:ready'
const ARTIFACT = 'open-notebook:artifact'

/**
 * Generic renderer for plugins that own their own view. The view HTML is
 * fetched with auth, object-URL'd, and shown in a **sandboxed** iframe
 * (`allow-scripts` without `allow-same-origin`, so it runs as an opaque origin
 * and can never read the host's auth token). Two sources:
 *
 *  - **bundle** (`hasViewBundle`): a self-contained HTML view shipped inside the
 *    plugin package (`/creation/creators/{key}/view`). After it loads we post the
 *    artifact `{schema_id, data}` in; the bundle dispatches by `schema_id`, so a
 *    newer plugin still renders artifacts created under older schema versions.
 *  - **snapshot**: a per-artifact HTML file the plugin emitted at generation time
 *    (e.g. textbook's Quarto output). Already self-contained; nothing is injected.
 *
 * Non-view files are offered as host-side downloads (the sandboxed iframe can't
 * fetch authed endpoints itself).
 */
export function PluginViewRenderer({
  artifact,
  hasViewBundle,
}: {
  artifact: CreationArtifact
  hasViewBundle?: boolean
}) {
  const { t } = useTranslation()
  const files = artifact.files ?? []
  const htmlIndex = files.findIndex(
    f => f.content_type === 'text/html' || f.filename.toLowerCase().endsWith('.html')
  )
  // Snapshot mode shows the emitted HTML inline, so don't also list it as a download.
  const downloadFiles = files
    .map((file, idx) => ({ file, idx }))
    .filter(({ idx }) => hasViewBundle || idx !== htmlIndex)

  const iframeRef = useRef<HTMLIFrameElement | null>(null)
  const [previewUrl, setPreviewUrl] = useState<string | null>(null)
  const [previewError, setPreviewError] = useState(false)

  // Fetch the view HTML (bundle or snapshot) with auth, then object-URL it.
  useEffect(() => {
    if (!hasViewBundle && htmlIndex < 0) return
    let cancelled = false
    let url: string | null = null
    setPreviewError(false)
    ;(async () => {
      try {
        const blob = hasViewBundle
          ? await creationApi.getCreatorView(artifact.creator_key)
          : await creationApi.downloadFile(artifact.id, htmlIndex)
        if (cancelled) return
        url = URL.createObjectURL(blob)
        setPreviewUrl(url)
      } catch {
        if (!cancelled) setPreviewError(true)
      }
    })()
    return () => {
      cancelled = true
      if (url) URL.revokeObjectURL(url)
    }
  }, [artifact.id, artifact.creator_key, hasViewBundle, htmlIndex])

  // Post the artifact into the bundle. Includes `config` (e.g. theme choice) and a
  // resolved light/dark `theme` — the opaque-origin iframe can't read the host's
  // dark-mode class itself. Display-only: data flows in, nothing comes back.
  const postArtifact = useCallback(() => {
    const win = iframeRef.current?.contentWindow
    if (!win) return
    const dark =
      typeof document !== 'undefined' &&
      document.documentElement.classList.contains('dark')
    win.postMessage(
      {
        type: ARTIFACT,
        schema_id: artifact.schema_id,
        name: artifact.name,
        data: artifact.data,
        config: artifact.config ?? {},
        theme: dark ? 'dark' : 'light',
      },
      '*'
    )
  }, [artifact.schema_id, artifact.name, artifact.data, artifact.config])

  // Bundle mode: inject the artifact once the view is live. Post on iframe `load`
  // and again whenever the bundle announces `ready`, covering either order.
  useEffect(() => {
    if (!hasViewBundle || !previewUrl) return
    const onMessage = (e: MessageEvent) => {
      if (e.source === iframeRef.current?.contentWindow && e.data?.type === READY) {
        postArtifact()
      }
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [hasViewBundle, previewUrl, postArtifact])

  const onDownload = async (idx: number) => {
    const blob = await creationApi.downloadFile(artifact.id, idx)
    triggerBrowserDownload(blob, files[idx].filename)
  }

  const showFrame = hasViewBundle || htmlIndex >= 0

  return (
    <div className="space-y-4">
      {(previewUrl || downloadFiles.length > 0) && (
        <div className="flex flex-wrap justify-end gap-2">
          {previewUrl && (
            <Button
              size="sm"
              variant="outline"
              onClick={() => window.open(previewUrl, '_blank')}
            >
              <ExternalLink className="h-4 w-4" />
              {t('creation.textbook.openInNewTab')}
            </Button>
          )}
          {downloadFiles.map(({ file, idx }) => (
            <Button key={idx} size="sm" variant="outline" onClick={() => onDownload(idx)}>
              <Download className="h-4 w-4" />
              {file.label || file.filename}
            </Button>
          ))}
        </div>
      )}

      {showFrame &&
        (previewError ? (
          <p className="text-sm text-destructive">
            {t('creation.textbook.previewUnavailable')}
          </p>
        ) : previewUrl ? (
          <iframe
            ref={iframeRef}
            title={artifact.name}
            src={previewUrl}
            onLoad={() => {
              if (hasViewBundle) postArtifact()
            }}
            sandbox="allow-scripts allow-popups allow-modals"
            className="h-[600px] w-full rounded-md border bg-white"
          />
        ) : (
          <p className="text-sm text-muted-foreground">
            {t('creation.textbook.loadingPreview')}
          </p>
        ))}
    </div>
  )
}
