'use client'

import { useEffect, useMemo, useRef } from 'react'
import { Download } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { triggerBrowserDownload } from '@/lib/api/creation'
import { CreationArtifact } from '@/lib/types/creation'
import { ChartSpecV1Schema } from '@/lib/types/creation.generated'

/**
 * Renders one AntV G2 chart. @antv/g2 touches `document`/`canvas`, so it is
 * imported dynamically inside an effect — never at module scope — to stay
 * SSR/build-safe. Always destroys on unmount and guards late async resolution.
 */
function ChartCanvas({ spec }: { spec: Record<string, unknown> }) {
  const ref = useRef<HTMLDivElement>(null)
  const { t } = useTranslation()

  useEffect(() => {
    let cancelled = false
    let chart: { destroy: () => void } | null = null

    ;(async () => {
      try {
        const { Chart } = await import('@antv/g2')
        if (cancelled || !ref.current) return
        const c = new Chart({ container: ref.current, autoFit: true, height: 320 })
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
  }, [spec, t])

  const onExportPng = () => {
    const canvas = ref.current?.querySelector('canvas')
    if (!canvas) return
    canvas.toBlob(blob => {
      if (blob) triggerBrowserDownload(blob, 'infographic.png')
    })
  }

  return (
    <div className="space-y-2">
      <div ref={ref} data-testid="chart-canvas" className="min-h-[320px] w-full" />
      <div className="flex justify-end">
        <Button size="sm" variant="outline" onClick={onExportPng}>
          <Download className="h-4 w-4" />
          {t('creation.infographics.exportPng')}
        </Button>
      </div>
    </div>
  )
}

export function ChartSpecRenderer({ artifact }: { artifact: CreationArtifact }) {
  const { t } = useTranslation()
  const parsed = useMemo(() => ChartSpecV1Schema.safeParse(artifact.data), [artifact.data])

  if (!parsed.success) {
    return <p className="text-sm text-destructive">{t('creation.invalidArtifactData')}</p>
  }

  const title = parsed.data.title
  const specs = parsed.data.specs ?? []

  return (
    <div className="space-y-6">
      {title && <h3 className="text-lg font-semibold">{title}</h3>}
      {specs.map((spec, i) => (
        <ChartCanvas key={i} spec={spec as Record<string, unknown>} />
      ))}
    </div>
  )
}
