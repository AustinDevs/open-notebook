'use client'

import { useEffect, useMemo, useRef } from 'react'
import { Download } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { triggerBrowserDownload } from '@/lib/api/creation'
import { CreationArtifact } from '@/lib/types/creation'
import { ChartSpecV1Schema } from '@/lib/types/creation.generated'

// Resolve the stored theme to an actual AntV G2 theme name. "auto" follows the
// app's current light/dark mode (next-themes adds a `dark` class on <html>).
function resolveTheme(theme: unknown): string {
  if (!theme || theme === 'auto') {
    const dark =
      typeof document !== 'undefined' &&
      document.documentElement.classList.contains('dark')
    return dark ? 'dark' : 'light'
  }
  return String(theme)
}

/**
 * Renders one AntV G2 chart. @antv/g2 touches `document`/`canvas`, so it is
 * imported dynamically inside an effect — never at module scope — to stay
 * SSR/build-safe. Always destroys on unmount and guards late async resolution.
 */
function ChartCanvas({
  spec,
  theme,
  name,
}: {
  spec: Record<string, unknown>
  theme: string
  name: string
}) {
  const ref = useRef<HTMLDivElement>(null)
  const { t } = useTranslation()

  useEffect(() => {
    let cancelled = false
    let chart: { destroy: () => void } | null = null

    ;(async () => {
      try {
        const { Chart } = await import('@antv/g2')
        if (cancelled || !ref.current) return
        const c = new Chart({
          container: ref.current,
          autoFit: true,
          height: 320,
          theme,
        })
        c.options(spec as Record<string, never>)
        c.render()
        chart = c
      } catch (err) {
        console.error('AntV render failed', err)
        if (ref.current) {
          ref.current.textContent = t('creation.infographics.renderError')
        }
      }
    })()

    return () => {
      cancelled = true
      chart?.destroy()
    }
  }, [spec, theme, t])

  const onExportPng = () => {
    const canvas = ref.current?.querySelector('canvas')
    if (!canvas) return
    canvas.toBlob(blob => {
      if (blob) triggerBrowserDownload(blob, `${name}.png`)
    })
  }

  // SVG export renders an off-screen chart with the SVG renderer and serializes
  // the resulting <svg> (the on-screen chart uses canvas so PNG stays cheap).
  const onExportSvg = async () => {
    const [{ Chart }, gsvg] = await Promise.all([
      import('@antv/g2'),
      import('@antv/g-svg'),
    ])
    const tmp = document.createElement('div')
    tmp.style.cssText = 'position:fixed;left:-99999px;top:0;width:800px;height:480px'
    document.body.appendChild(tmp)
    let chart: { destroy: () => void } | null = null
    try {
      const c = new Chart({
        container: tmp,
        width: 800,
        height: 480,
        theme,
        renderer: new gsvg.Renderer(),
      })
      c.options(spec as Record<string, never>)
      c.render()
      chart = c
      // Wait for the SVG nodes to be attached.
      let svg: SVGElement | null = null
      for (let i = 0; i < 20 && !svg; i++) {
        svg = tmp.querySelector('svg')
        if (svg) break
        await new Promise(r => setTimeout(r, 50))
      }
      if (svg) {
        const data = new XMLSerializer().serializeToString(svg)
        triggerBrowserDownload(
          new Blob([data], { type: 'image/svg+xml;charset=utf-8' }),
          `${name}.svg`
        )
      }
    } catch (err) {
      console.error('SVG export failed', err)
    } finally {
      chart?.destroy()
      tmp.remove()
    }
  }

  return (
    <div className="space-y-2">
      <div ref={ref} data-testid="chart-canvas" className="min-h-[320px] w-full" />
      <div className="flex justify-end gap-2">
        <Button size="sm" variant="outline" onClick={onExportPng}>
          <Download className="h-4 w-4" />
          {t('creation.infographics.exportPng')}
        </Button>
        <Button size="sm" variant="outline" onClick={onExportSvg}>
          <Download className="h-4 w-4" />
          {t('creation.infographics.exportSvg')}
        </Button>
      </div>
    </div>
  )
}

export function ChartSpecRenderer({ artifact }: { artifact: CreationArtifact }) {
  const { t } = useTranslation()
  const parsed = useMemo(() => ChartSpecV1Schema.safeParse(artifact.data), [artifact.data])
  const theme = resolveTheme(artifact.config?.theme)
  const baseName = artifact.name?.replace(/[^A-Za-z0-9._-]+/g, '_') || 'infographic'

  if (!parsed.success) {
    return <p className="text-sm text-destructive">{t('creation.invalidArtifactData')}</p>
  }

  const title = parsed.data.title
  const specs = parsed.data.specs ?? []

  return (
    <div className="space-y-6">
      {title && <h3 className="text-lg font-semibold">{title}</h3>}
      {specs.map((spec, i) => (
        <ChartCanvas
          key={i}
          spec={spec as Record<string, unknown>}
          theme={theme}
          name={specs.length > 1 ? `${baseName}-${i + 1}` : baseName}
        />
      ))}
    </div>
  )
}
