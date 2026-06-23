'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import { Download, ExternalLink } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { creationApi, triggerBrowserDownload } from '@/lib/api/creation'
import { CreationArtifact } from '@/lib/types/creation'

const READY = 'open-notebook:ready'
const ARTIFACT = 'open-notebook:artifact'

// The preview iframe is sandboxed without `allow-same-origin`, so its document
// runs at an opaque (`null`) origin where the History API is forbidden. Decks
// that write their position to the URL (reveal.js calls `history.replaceState`
// during `initialize()`) otherwise throw a SecurityError that aborts init before
// anything paints — a blank white frame. Wrapping the History methods to swallow
// that error lets such views render; it's a harmless no-op at a real origin.
const HISTORY_GUARD =
  '<script>(function(){try{var h=window.history;["pushState","replaceState"].forEach(function(m){var o=h[m];if(typeof o!=="function")return;h[m]=function(){try{return o.apply(h,arguments)}catch(e){}}})}catch(e){}})();</script>'

// iframe-resizer's child agent, injected into bundle views so they report their
// content height back to the parent. CDN-loaded (same source/major the notebook
// page uses) to avoid adding an npm dependency.
const RESIZER_CHILD =
  '<script src="https://cdn.jsdelivr.net/npm/@iframe-resizer/child@5"></script>'

/** Insert a snippet just inside `<head>` (then `<html>`, else prepend). */
function injectAtHead(html: string, snippet: string): string {
  const head = html.match(/<head[^>]*>/i)
  if (head) {
    const at = head.index! + head[0].length
    return html.slice(0, at) + snippet + html.slice(at)
  }
  const htmlTag = html.match(/<html[^>]*>/i)
  if (htmlTag) {
    const at = htmlTag.index! + htmlTag[0].length
    return html.slice(0, at) + snippet + html.slice(at)
  }
  return snippet + html
}

/**
 * Prepare fetched view HTML for the sandboxed iframe: always inject the history
 * guard (runs before any view script). For bundle views, also inject the
 * iframe-resizer child at end of body so the frame can auto-size to content.
 */
function prepareViewHtml(html: string, autoResize: boolean): string {
  let out = injectAtHead(html, HISTORY_GUARD)
  if (autoResize) {
    const close = out.match(/<\/body>/i)
    out = close
      ? out.slice(0, close.index!) + RESIZER_CHILD + out.slice(close.index!)
      : out + RESIZER_CHILD
  }
  return out
}

// Load iframe-resizer's parent from CDN once, on demand. The notebook iframe uses
// the same source, so this adds no dependency. Resolves when `window.iframeResize`
// is available.
let resizerParentPromise: Promise<void> | null = null
function loadResizerParent(): Promise<void> {
  if (typeof window === 'undefined') return Promise.resolve()
  if ((window as unknown as { iframeResize?: unknown }).iframeResize) return Promise.resolve()
  if (resizerParentPromise) return resizerParentPromise
  resizerParentPromise = new Promise<void>((resolve, reject) => {
    const s = document.createElement('script')
    s.src = 'https://cdn.jsdelivr.net/npm/@iframe-resizer/parent@5'
    s.async = true
    s.onload = () => resolve()
    s.onerror = () => {
      resizerParentPromise = null
      reject(new Error('iframe-resizer failed to load'))
    }
    document.head.appendChild(s)
  })
  return resizerParentPromise
}

type ResizableIframe = HTMLIFrameElement & { iFrameResizer?: { close: () => void } }

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
        const guarded = new Blob([prepareViewHtml(await blob.text(), !!hasViewBundle)], { type: 'text/html' })
        url = URL.createObjectURL(guarded)
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

  // Auto-size bundle views to their content height (the injected iframe-resizer
  // child reports it). Snapshot HTML — e.g. paged reveal.js slideshows — keeps a
  // fixed height, so it's intentionally excluded. `checkOrigin: false` because the
  // frame runs at an opaque (sandboxed) origin.
  useEffect(() => {
    if (!hasViewBundle || !previewUrl) return
    let cancelled = false
    let connected: ResizableIframe | null = null
    loadResizerParent()
      .then(() => {
        if (cancelled) return
        const el = iframeRef.current as ResizableIframe | null
        const iframeResize = (
          window as unknown as {
            iframeResize?: (opts: object, el: HTMLIFrameElement) => unknown
          }
        ).iframeResize
        if (!el || !iframeResize || el.iFrameResizer) return
        iframeResize({ license: 'GPLv3', checkOrigin: false, waitForLoad: true }, el)
        connected = el
      })
      .catch(() => {})
    return () => {
      cancelled = true
      try {
        connected?.iFrameResizer?.close()
      } catch {
        /* already torn down */
      }
    }
  }, [hasViewBundle, previewUrl])

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
            className={
              hasViewBundle
                ? 'min-h-[120px] w-full rounded-md border bg-white'
                : 'h-[600px] w-full rounded-md border bg-white'
            }
          />
        ) : (
          <p className="text-sm text-muted-foreground">
            {t('creation.textbook.loadingPreview')}
          </p>
        ))}
    </div>
  )
}
