import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'

import { FlashcardsV1Renderer } from './FlashcardsV1Renderer'
import type { CreationArtifact } from '@/lib/types/creation'

// --- mocks ------------------------------------------------------------------
// `reviewData` must be a STABLE reference: react-query returns a stable object,
// and the renderer's useEffect depends on it — a fresh {} each render would loop.
const { saveMutate, downloadFile, triggerBrowserDownload, reviewData } = vi.hoisted(
  () => ({
    saveMutate: vi.fn(),
    downloadFile: vi.fn(),
    triggerBrowserDownload: vi.fn(),
    reviewData: {} as Record<string, Record<string, unknown>>,
  })
)
vi.mock('@/lib/hooks/use-creation', () => ({
  useFlashcardReview: () => ({ data: reviewData }),
  useSaveFlashcardReview: () => ({ mutate: saveMutate }),
}))
vi.mock('@/lib/api/creation', () => ({
  creationApi: { downloadFile },
  triggerBrowserDownload,
}))

function makeArtifact(overrides: Partial<CreationArtifact> = {}): CreationArtifact {
  return {
    id: 'creation_artifact:1',
    creator_key: 'flashcards',
    schema_id: 'flashcards.v1',
    name: 'Bio deck',
    status: 'completed',
    data: {
      deck_name: 'Bio',
      cards: [
        { id: 'c1', front: 'What is ATP?', back: 'Energy currency', tags: [] },
        { id: 'c2', front: 'Where photosynthesis?', back: 'Chloroplast', tags: [] },
      ],
    },
    files: [
      { filename: 'Bio.apkg', content_type: 'application/octet-stream', path: 'Bio.apkg', label: 'anki_deck' },
    ],
    config: {},
    warnings: [],
    errors: [],
    ...overrides,
  }
}

describe('FlashcardsV1Renderer', () => {
  beforeEach(() => {
    saveMutate.mockClear()
    downloadFile.mockClear()
    downloadFile.mockResolvedValue(new Blob(['x']))
    triggerBrowserDownload.mockClear()
  })

  it('lists all cards in list mode', () => {
    render(<FlashcardsV1Renderer artifact={makeArtifact()} />)
    expect(screen.getByText('What is ATP?')).toBeDefined()
    expect(screen.getByText('Energy currency')).toBeDefined()
    expect(screen.getByText('Where photosynthesis?')).toBeDefined()
  })

  it('shows a download button when an .apkg file is present', () => {
    render(<FlashcardsV1Renderer artifact={makeArtifact()} />)
    expect(screen.getByText('creation.flashcards.downloadApkg')).toBeDefined()
  })

  it('hides the download button when there is no .apkg file', () => {
    render(<FlashcardsV1Renderer artifact={makeArtifact({ files: [] })} />)
    expect(screen.queryByText('creation.flashcards.downloadApkg')).toBeNull()
  })

  it('downloads the deck when the button is clicked', () => {
    render(<FlashcardsV1Renderer artifact={makeArtifact()} />)
    fireEvent.click(screen.getByText('creation.flashcards.downloadApkg'))
    expect(downloadFile).toHaveBeenCalledWith('creation_artifact:1', 0)
  })

  it('always offers Print and CSV export, even without an .apkg', () => {
    render(<FlashcardsV1Renderer artifact={makeArtifact({ files: [] })} />)
    expect(screen.getByText('creation.flashcards.print')).toBeDefined()
    expect(screen.getByText('creation.flashcards.exportCsv')).toBeDefined()
  })

  it('exports cards as CSV when the CSV button is clicked', () => {
    render(<FlashcardsV1Renderer artifact={makeArtifact()} />)
    fireEvent.click(screen.getByText('creation.flashcards.exportCsv'))
    expect(triggerBrowserDownload).toHaveBeenCalledTimes(1)
    const [blob, filename] = triggerBrowserDownload.mock.calls[0]
    expect(filename.endsWith('.csv')).toBe(true)
    expect(blob).toBeInstanceOf(Blob)
  })

  it('runs the study flow: reveal then rate, persisting review state', () => {
    render(<FlashcardsV1Renderer artifact={makeArtifact()} />)

    // enter study mode
    fireEvent.click(screen.getByText('creation.flashcards.study'))
    expect(screen.getByText('creation.flashcards.reveal')).toBeDefined()

    // reveal the answer -> rating buttons appear
    fireEvent.click(screen.getByText('creation.flashcards.reveal'))
    expect(screen.getByText('creation.flashcards.good')).toBeDefined()

    // rate the card -> scheduling state saved
    fireEvent.click(screen.getByText('creation.flashcards.good'))
    expect(saveMutate).toHaveBeenCalledTimes(1)
    const saved = saveMutate.mock.calls[0][0] as Record<string, unknown>
    expect(Object.keys(saved)).toContain('c1')
  })

  it('shows an error for invalid artifact data', () => {
    render(<FlashcardsV1Renderer artifact={makeArtifact({ data: { nope: true } })} />)
    expect(screen.getByText('creation.invalidArtifactData')).toBeDefined()
  })
})
