'use client'

import { useMemo, useRef } from 'react'
import { Download, Quote } from 'lucide-react'
import * as Icons from 'lucide-react'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { triggerBrowserDownload } from '@/lib/api/creation'
import { CreationArtifact } from '@/lib/types/creation'
import { InfographicV1Schema } from '@/lib/types/creation.generated'

// "auto" follows the app's light/dark mode (next-themes adds a `dark` class).
function resolveTheme(theme: unknown): 'light' | 'dark' {
  if (theme === 'light' || theme === 'dark') return theme
  if (typeof document !== 'undefined' && document.documentElement.classList.contains('dark')) {
    return 'dark'
  }
  return 'light'
}

// Map a lucide icon name (e.g. "trending-up") to its component, if it exists.
function lucideIcon(name?: string | null) {
  if (!name) return null
  const pascal = name
    .split(/[-_ ]+/)
    .map(p => p.charAt(0).toUpperCase() + p.slice(1))
    .join('')
  const Comp = (Icons as Record<string, unknown>)[pascal]
  return typeof Comp === 'function' ? (Comp as React.ComponentType<{ className?: string }>) : null
}

export function InfographicRenderer({ artifact }: { artifact: CreationArtifact }) {
  const { t } = useTranslation()
  const ref = useRef<HTMLDivElement>(null)
  const parsed = useMemo(() => InfographicV1Schema.safeParse(artifact.data), [artifact.data])
  const theme = resolveTheme(artifact.config?.theme)
  const baseName = artifact.name?.replace(/[^A-Za-z0-9._-]+/g, '_') || 'infographic'

  if (!parsed.success) {
    return <p className="text-sm text-destructive">{t('creation.invalidArtifactData')}</p>
  }

  const { title, subtitle } = parsed.data
  const blocks = parsed.data.blocks ?? []

  const dark = theme === 'dark'
  const surface = dark ? 'bg-zinc-900 text-zinc-100' : 'bg-white text-zinc-900'
  const card = dark ? 'bg-zinc-800 border-zinc-700' : 'bg-zinc-50 border-zinc-200'
  const muted = dark ? 'text-zinc-400' : 'text-zinc-500'

  const exportImage = async (fmt: 'png' | 'svg') => {
    if (!ref.current) return
    const mod = await import('html-to-image')
    const fn = fmt === 'png' ? mod.toPng : mod.toSvg
    const dataUrl = await fn(ref.current, { pixelRatio: 2, backgroundColor: dark ? '#18181b' : '#ffffff' })
    const blob = await (await fetch(dataUrl)).blob()
    triggerBrowserDownload(blob, `${baseName}.${fmt}`)
  }

  return (
    <div className="space-y-3">
      <div ref={ref} className={`rounded-xl border p-6 ${surface}`}>
        <header className="mb-5 space-y-1">
          <h2 className="text-2xl font-bold tracking-tight">{title}</h2>
          {subtitle && <p className={`text-sm ${muted}`}>{subtitle}</p>}
        </header>

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          {blocks.map((b, i) => {
            if (b.type === 'stat') {
              const Icon = lucideIcon(b.icon)
              return (
                <div key={i} className={`rounded-lg border p-4 ${card}`}>
                  <div className="flex items-center gap-2">
                    {Icon && <Icon className="h-5 w-5" />}
                    <div className="text-3xl font-extrabold leading-none">{b.value}</div>
                  </div>
                  {b.label && <div className="mt-2 text-sm font-medium">{b.label}</div>}
                  {b.description && <div className={`mt-1 text-xs ${muted}`}>{b.description}</div>}
                </div>
              )
            }
            if (b.type === 'text') {
              return (
                <div key={i} className={`rounded-lg border p-4 ${card}`}>
                  {b.heading && <div className="mb-1 text-sm font-semibold">{b.heading}</div>}
                  <p className="text-sm">{b.body}</p>
                </div>
              )
            }
            if (b.type === 'list') {
              return (
                <div key={i} className={`rounded-lg border p-4 ${card}`}>
                  {b.heading && <div className="mb-2 text-sm font-semibold">{b.heading}</div>}
                  <ul className="list-disc space-y-1 pl-5 text-sm">
                    {(b.items ?? []).map((it, j) => (
                      <li key={j}>{it}</li>
                    ))}
                  </ul>
                </div>
              )
            }
            if (b.type === 'quote') {
              return (
                <div key={i} className={`rounded-lg border p-4 sm:col-span-2 ${card}`}>
                  <Quote className={`mb-1 h-4 w-4 ${muted}`} />
                  <blockquote className="text-base italic">{b.text}</blockquote>
                  {b.attribution && <div className={`mt-2 text-xs ${muted}`}>— {b.attribution}</div>}
                </div>
              )
            }
            return null
          })}
        </div>
      </div>

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
