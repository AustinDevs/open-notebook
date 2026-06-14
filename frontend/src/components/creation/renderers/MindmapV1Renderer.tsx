'use client'

import { useEffect, useMemo, useRef, useState } from 'react'
import { Download } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { triggerBrowserDownload } from '@/lib/api/creation'
import { CreationArtifact } from '@/lib/types/creation'
import { MindmapV1Schema } from '@/lib/types/creation.generated'

// Unique, DOM-id-safe handle for each mermaid.render() call (ids with ":" or "."
// break mermaid's internal selectors, so React's useId() isn't usable directly).
let _mermaidSeq = 0

// "auto"-style theming: mermaid's "dark" theme follows the app's dark mode
// (next-themes toggles a `dark` class on <html>); otherwise use the default.
function resolveMermaidTheme(): 'dark' | 'default' {
  const dark =
    typeof document !== 'undefined' &&
    document.documentElement.classList.contains('dark')
  return dark ? 'dark' : 'default'
}

// Ensure a serialized <svg> carries the xmlns so it opens standalone (export).
function serializeSvg(svg: SVGElement): string {
  const clone = svg.cloneNode(true) as SVGElement
  if (!clone.getAttribute('xmlns')) {
    clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg')
  }
  return new XMLSerializer().serializeToString(clone)
}

/**
 * Renders a mermaid `mindmap`. mermaid touches `document`, so it is imported
 * dynamically inside an effect — never at module scope — to stay SSR/build-safe.
 * Guards late async resolution and re-renders on theme change.
 */
function MindmapCanvas({
  syntax,
  name,
}: {
  syntax: string
  name: string
}) {
  const ref = useRef<HTMLDivElement>(null)
  const { t } = useTranslation()
  const [error, setError] = useState(false)
  const theme = resolveMermaidTheme()

  useEffect(() => {
    let cancelled = false

    ;(async () => {
      try {
        const mermaid = (await import('mermaid')).default
        if (cancelled || !ref.current) return
        mermaid.initialize({
          startOnLoad: false,
          theme,
          securityLevel: 'strict',
        })
        const id = `mindmap-svg-${_mermaidSeq++}`
        const { svg } = await mermaid.render(id, syntax)
        if (cancelled || !ref.current) return
        ref.current.innerHTML = svg
        setError(false)
      } catch (err) {
        console.error('mermaid render failed', err)
        if (!cancelled) {
          if (ref.current) ref.current.innerHTML = ''
          setError(true)
        }
      }
    })()

    return () => {
      cancelled = true
    }
  }, [syntax, theme])

  const onExportMarkdown = () => {
    const md = '```mermaid\n' + syntax.trimEnd() + '\n```\n'
    triggerBrowserDownload(
      new Blob([md], { type: 'text/markdown;charset=utf-8' }),
      `${name}.md`
    )
  }

  const onExportSvg = () => {
    const svg = ref.current?.querySelector('svg')
    if (!svg) return
    triggerBrowserDownload(
      new Blob([serializeSvg(svg)], { type: 'image/svg+xml;charset=utf-8' }),
      `${name}.svg`
    )
  }

  const onExportPng = () => {
    const svg = ref.current?.querySelector('svg')
    if (!svg) return
    const rect = svg.getBoundingClientRect()
    const data = serializeSvg(svg)
    const url = URL.createObjectURL(
      new Blob([data], { type: 'image/svg+xml;charset=utf-8' })
    )
    const img = new Image()
    img.onload = () => {
      const scale = 2
      const w = rect.width || img.width
      const h = rect.height || img.height
      const canvas = document.createElement('canvas')
      canvas.width = Math.max(1, Math.round(w * scale))
      canvas.height = Math.max(1, Math.round(h * scale))
      const ctx = canvas.getContext('2d')
      if (ctx) {
        ctx.scale(scale, scale)
        ctx.drawImage(img, 0, 0, w, h)
        canvas.toBlob(blob => {
          if (blob) triggerBrowserDownload(blob, `${name}.png`)
        }, 'image/png')
      }
      URL.revokeObjectURL(url)
    }
    img.onerror = () => URL.revokeObjectURL(url)
    img.src = url
  }

  if (error) {
    return <p className="text-sm text-destructive">{t('creation.mindmap.renderError')}</p>
  }

  return (
    <div className="space-y-2">
      <div ref={ref} data-testid="mindmap-canvas" className="w-full overflow-x-auto" />
      <div className="flex flex-wrap justify-end gap-2">
        <Button size="sm" variant="outline" onClick={onExportMarkdown}>
          <Download className="h-4 w-4" />
          {t('creation.mindmap.exportMarkdown')}
        </Button>
        <Button size="sm" variant="outline" onClick={onExportPng}>
          <Download className="h-4 w-4" />
          {t('creation.mindmap.exportPng')}
        </Button>
        <Button size="sm" variant="outline" onClick={onExportSvg}>
          <Download className="h-4 w-4" />
          {t('creation.mindmap.exportSvg')}
        </Button>
      </div>
    </div>
  )
}

export function MindmapV1Renderer({ artifact }: { artifact: CreationArtifact }) {
  const { t } = useTranslation()
  const parsed = useMemo(() => MindmapV1Schema.safeParse(artifact.data), [artifact.data])
  const baseName = artifact.name?.replace(/[^A-Za-z0-9._-]+/g, '_') || 'mindmap'

  if (!parsed.success) {
    return <p className="text-sm text-destructive">{t('creation.invalidArtifactData')}</p>
  }

  const { title, mermaid_syntax } = parsed.data

  return (
    <div className="space-y-4">
      {title && <h3 className="text-lg font-semibold">{title}</h3>}
      <MindmapCanvas syntax={mermaid_syntax} name={baseName} />
    </div>
  )
}
