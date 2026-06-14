'use client'

import { useEffect, useMemo, useRef } from 'react'
import { Download } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { triggerBrowserDownload } from '@/lib/api/creation'
import { CreationArtifact } from '@/lib/types/creation'
import { InfographicV2Schema } from '@/lib/types/creation.generated'

// Resolve the stored theme to an AntV Infographic theme name. "auto" follows the
// app's current light/dark mode (next-themes adds a `dark` class on <html>). The
// DSL's own `theme` block (e.g. palette) still layers colour on top of this base.
function resolveTheme(theme: unknown): string {
  if (!theme || theme === 'auto') {
    const dark =
      typeof document !== 'undefined' &&
      document.documentElement.classList.contains('dark')
    return dark ? 'dark' : 'light'
  }
  return String(theme)
}

type InfographicInstance = {
  render: (spec: string) => void
  toDataURL: (opts: { type: 'png' | 'svg'; dpr?: number }) => Promise<string>
  destroy: () => void
}

/**
 * Renders an AntV Infographic from its declarative DSL `spec` (emitted by the
 * `infographics` and `charts` creators as `infographic.v2`). @antv/infographic
 * touches `document`, so it is imported dynamically inside an effect — never at
 * module scope — to stay SSR/build-safe. The instance is kept in a ref so the
 * export buttons can call its native `toDataURL`. Width is fluid (100%); height
 * follows the template's intrinsic viewBox.
 */
export function InfographicV2Renderer({ artifact }: { artifact: CreationArtifact }) {
  const { t } = useTranslation()
  const ref = useRef<HTMLDivElement>(null)
  const igRef = useRef<InfographicInstance | null>(null)
  const parsed = useMemo(() => InfographicV2Schema.safeParse(artifact.data), [artifact.data])
  const theme = resolveTheme(artifact.config?.theme)
  const baseName = artifact.name?.replace(/[^A-Za-z0-9._-]+/g, '_') || 'infographic'
  const spec = parsed.success ? parsed.data.spec ?? '' : ''

  useEffect(() => {
    if (!spec) return
    let cancelled = false
    let ig: InfographicInstance | null = null

    ;(async () => {
      try {
        const { Infographic } = await import('@antv/infographic')
        if (cancelled || !ref.current) return
        ig = new Infographic({
          container: ref.current,
          width: '100%',
          theme,
        }) as unknown as InfographicInstance
        ig.render(spec)
        igRef.current = ig
        // Text metrics depend on fonts; re-render once they are ready so labels
        // are laid out correctly (mirrors AntV's own usage guidance).
        document.fonts?.ready
          ?.then(() => {
            if (!cancelled) ig?.render(spec)
          })
          .catch(() => {})
      } catch (err) {
        console.error('AntV infographic render failed', err)
        if (ref.current) {
          ref.current.textContent = t('creation.infographics.renderError')
        }
      }
    })()

    return () => {
      cancelled = true
      igRef.current = null
      ig?.destroy()
    }
  }, [spec, theme, t])

  if (!parsed.success) {
    return <p className="text-sm text-destructive">{t('creation.invalidArtifactData')}</p>
  }

  const exportImage = async (fmt: 'png' | 'svg') => {
    const ig = igRef.current
    if (!ig) return
    try {
      const dataUrl = await ig.toDataURL(
        fmt === 'png' ? { type: 'png', dpr: 2 } : { type: 'svg' }
      )
      const blob = await (await fetch(dataUrl)).blob()
      triggerBrowserDownload(blob, `${baseName}.${fmt}`)
    } catch (err) {
      console.error('infographic export failed', err)
    }
  }

  return (
    <div className="space-y-3">
      {parsed.data.title && <h3 className="text-lg font-semibold">{parsed.data.title}</h3>}
      <div ref={ref} data-testid="infographic-canvas" className="min-h-[320px] w-full" />
      <div className="flex justify-end gap-2">
        <Button size="sm" variant="outline" onClick={() => exportImage('png')}>
          <Download className="h-4 w-4" />
          {t('creation.infographics.exportPng')}
        </Button>
        <Button size="sm" variant="outline" onClick={() => exportImage('svg')}>
          <Download className="h-4 w-4" />
          {t('creation.infographics.exportSvg')}
        </Button>
      </div>
    </div>
  )
}
