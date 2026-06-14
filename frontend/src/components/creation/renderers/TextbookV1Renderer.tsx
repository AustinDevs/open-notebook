'use client'

import { useEffect, useState } from 'react'
import { Download, ExternalLink } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { creationApi, triggerBrowserDownload } from '@/lib/api/creation'
import { CreationArtifact } from '@/lib/types/creation'

type TextbookData = {
  title?: string
  subtitle?: string | null
  chapters?: { title: string; summary?: string | null }[]
  formats?: string[]
}

/**
 * Renders a `textbook.v1` artifact: the chapter list plus the Quarto-rendered
 * files. The self-contained HTML is fetched as a blob and shown inline in an
 * iframe (the download endpoint returns bytes regardless of disposition); PDF /
 * EPUB are offered as downloads, labelled by the backend-supplied `file.label`.
 */
export function TextbookV1Renderer({ artifact }: { artifact: CreationArtifact }) {
  const { t } = useTranslation()
  const data = (artifact.data ?? {}) as TextbookData
  const files = artifact.files ?? []
  const htmlIndex = files.findIndex(
    f => f.content_type === 'text/html' || f.filename.toLowerCase().endsWith('.html')
  )

  const [previewUrl, setPreviewUrl] = useState<string | null>(null)
  const [previewError, setPreviewError] = useState(false)

  useEffect(() => {
    if (htmlIndex < 0) return
    let cancelled = false
    let url: string | null = null
    ;(async () => {
      try {
        const blob = await creationApi.downloadFile(artifact.id, htmlIndex)
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
  }, [artifact.id, htmlIndex])

  const onDownload = async (idx: number) => {
    const blob = await creationApi.downloadFile(artifact.id, idx)
    triggerBrowserDownload(blob, files[idx].filename)
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          {data.title && <h3 className="text-lg font-semibold">{data.title}</h3>}
          {data.subtitle && (
            <p className="text-sm text-muted-foreground">{data.subtitle}</p>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
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
          {files.map((file, idx) => (
            <Button key={idx} size="sm" variant="outline" onClick={() => onDownload(idx)}>
              <Download className="h-4 w-4" />
              {file.label || file.filename}
            </Button>
          ))}
        </div>
      </div>

      {Array.isArray(data.chapters) && data.chapters.length > 0 && (
        <div>
          <div className="mb-1 text-sm font-medium">{t('creation.textbook.chapters')}</div>
          <ol className="list-decimal space-y-0.5 pl-5 text-sm text-muted-foreground">
            {data.chapters.map((c, i) => (
              <li key={i}>{c.title}</li>
            ))}
          </ol>
        </div>
      )}

      {htmlIndex >= 0 &&
        (previewError ? (
          <p className="text-sm text-destructive">
            {t('creation.textbook.previewUnavailable')}
          </p>
        ) : previewUrl ? (
          <iframe
            title={data.title || 'textbook'}
            src={previewUrl}
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
