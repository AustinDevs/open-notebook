'use client'

import { useEffect, useMemo, useState } from 'react'
import { Download, RotateCcw } from 'lucide-react'
import {
  createEmptyCard,
  fsrs,
  generatorParameters,
  Rating,
  type Card as FsrsCard,
  type Grade,
} from 'ts-fsrs'

import { Button } from '@/components/ui/button'
import { useTranslation } from '@/lib/hooks/use-translation'
import { creationApi, triggerBrowserDownload } from '@/lib/api/creation'
import { useFlashcardReview, useSaveFlashcardReview } from '@/lib/hooks/use-creation'
import { CreationArtifact } from '@/lib/types/creation'
import { FlashcardsV1Schema } from '@/lib/types/creation.generated'

const f = fsrs(generatorParameters({ enable_fuzz: true }))

type ReviewMap = Record<string, FsrsCard>

function reviveCard(state: Record<string, unknown> | undefined): FsrsCard {
  if (!state) return createEmptyCard(new Date())
  const c = { ...(state as unknown as FsrsCard) }
  c.due = new Date(c.due)
  c.last_review = c.last_review ? new Date(c.last_review) : undefined
  return c
}

type Card = { id: string; front: string; back: string; tags?: string[] }

function htmlEscape(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
}

// Open a print-friendly window (front/back per card) and trigger the browser's
// print dialog — users can print on paper or "Save as PDF".
function printFlashcards(deckName: string, cards: Card[]) {
  const w = window.open('', '_blank')
  if (!w) return
  const rows = cards
    .map(
      (c, i) =>
        `<div class="card"><div class="q">${i + 1}. ${htmlEscape(c.front)}</div>` +
        `<div class="a">${htmlEscape(c.back)}</div></div>`
    )
    .join('')
  w.document.write(
    `<!doctype html><html><head><meta charset="utf-8"><title>${htmlEscape(deckName)}</title>` +
      `<style>body{font-family:system-ui,sans-serif;margin:2rem;color:#111}` +
      `h1{font-size:1.4rem}.card{border:1px solid #ddd;border-radius:8px;padding:12px 16px;` +
      `margin:10px 0;page-break-inside:avoid}.q{font-weight:600;margin-bottom:6px}` +
      `.a{color:#444}@media print{.card{break-inside:avoid}}</style></head><body>` +
      `<h1>${htmlEscape(deckName)}</h1>${rows}` +
      `<script>window.onload=function(){window.print()}</script></body></html>`
  )
  w.document.close()
}

function csvCell(s: string): string {
  return `"${s.replace(/"/g, '""')}"`
}

function exportCsv(deckName: string, cards: Card[]) {
  const lines = [['front', 'back', 'tags'].join(',')]
  for (const c of cards) {
    lines.push(
      [csvCell(c.front), csvCell(c.back), csvCell((c.tags ?? []).join('|'))].join(',')
    )
  }
  const safe = deckName.replace(/[^A-Za-z0-9._-]+/g, '_') || 'flashcards'
  triggerBrowserDownload(
    new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' }),
    `${safe}.csv`
  )
}

export function FlashcardsRenderer({ artifact }: { artifact: CreationArtifact }) {
  const { t } = useTranslation()
  const parsed = useMemo(() => FlashcardsV1Schema.safeParse(artifact.data), [artifact.data])
  const cards = useMemo(() => (parsed.success ? parsed.data.cards : []), [parsed])
  const deckName = (parsed.success ? parsed.data.deck_name : artifact.name) || artifact.name

  const { data: storedReview } = useFlashcardReview(artifact.id)
  const saveReview = useSaveFlashcardReview(artifact.id)

  const [reviews, setReviews] = useState<ReviewMap>({})
  const [studying, setStudying] = useState(false)
  const [revealed, setRevealed] = useState(false)

  useEffect(() => {
    const map: ReviewMap = {}
    for (const c of cards) {
      map[c.id] = reviveCard(storedReview?.[c.id])
    }
    setReviews(map)
  }, [storedReview, cards])

  const dueQueue = useMemo(() => {
    const now = Date.now()
    return cards
      .filter(c => reviews[c.id] && +new Date(reviews[c.id].due) <= now)
      .sort((a, b) => +new Date(reviews[a.id].due) - +new Date(reviews[b.id].due))
      .map(c => c.id)
  }, [cards, reviews])

  const currentId = dueQueue[0]
  const current = cards.find(c => c.id === currentId)

  const rate = (rating: Grade) => {
    if (!currentId) return
    const now = new Date()
    const next = f.next(reviews[currentId], now, rating).card
    const updated = { ...reviews, [currentId]: next }
    setReviews(updated)
    setRevealed(false)
    // Persist this card's state.
    saveReview.mutate({ [currentId]: next as unknown as Record<string, unknown> })
  }

  const apkgIndex = artifact.files.findIndex(file => file.filename.endsWith('.apkg'))

  const onDownload = async () => {
    if (apkgIndex < 0) return
    const blob = await creationApi.downloadFile(artifact.id, apkgIndex)
    triggerBrowserDownload(blob, artifact.files[apkgIndex].filename)
  }

  if (!parsed.success) {
    return <p className="text-sm text-destructive">{t('creation.invalidArtifactData')}</p>
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="text-sm text-muted-foreground">
          {t('creation.flashcards.cardCount').replace('{count}', String(cards.length))}
          {dueQueue.length > 0 && ` · ${dueQueue.length} ${t('creation.flashcards.due')}`}
        </div>
        <div className="flex gap-2">
          <Button
            size="sm"
            variant={studying ? 'secondary' : 'default'}
            onClick={() => {
              setStudying(s => !s)
              setRevealed(false)
            }}
          >
            {studying ? t('creation.flashcards.exitStudy') : t('creation.flashcards.study')}
          </Button>
          {apkgIndex >= 0 && (
            <Button size="sm" variant="outline" onClick={onDownload}>
              <Download className="h-4 w-4" />
              {t('creation.flashcards.downloadApkg')}
            </Button>
          )}
          <Button
            size="sm"
            variant="outline"
            onClick={() => printFlashcards(deckName, cards)}
          >
            {t('creation.flashcards.print')}
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => exportCsv(deckName, cards)}
          >
            {t('creation.flashcards.exportCsv')}
          </Button>
        </div>
      </div>

      {studying ? (
        <div className="rounded-lg border p-6">
          {current ? (
            <div className="space-y-4">
              <div className="text-base font-medium">{current.front}</div>
              {revealed ? (
                <>
                  <div className="border-t pt-3 text-sm text-muted-foreground">
                    {current.back}
                  </div>
                  <div className="flex flex-wrap gap-2 pt-2">
                    <Button size="sm" variant="destructive" onClick={() => rate(Rating.Again)}>
                      {t('creation.flashcards.again')}
                    </Button>
                    <Button size="sm" variant="outline" onClick={() => rate(Rating.Hard)}>
                      {t('creation.flashcards.hard')}
                    </Button>
                    <Button size="sm" variant="outline" onClick={() => rate(Rating.Good)}>
                      {t('creation.flashcards.good')}
                    </Button>
                    <Button size="sm" onClick={() => rate(Rating.Easy)}>
                      {t('creation.flashcards.easy')}
                    </Button>
                  </div>
                </>
              ) : (
                <Button size="sm" variant="secondary" onClick={() => setRevealed(true)}>
                  {t('creation.flashcards.reveal')}
                </Button>
              )}
            </div>
          ) : (
            <div className="flex flex-col items-center gap-2 py-6 text-center text-muted-foreground">
              <RotateCcw className="h-6 w-6" />
              <p>{t('creation.flashcards.allDone')}</p>
            </div>
          )}
        </div>
      ) : (
        <ul className="divide-y rounded-lg border">
          {cards.map(card => (
            <li key={card.id} className="space-y-1 p-3">
              <div className="text-sm font-medium">{card.front}</div>
              <div className="text-sm text-muted-foreground">{card.back}</div>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
