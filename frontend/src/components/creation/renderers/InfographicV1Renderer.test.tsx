import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'

import { InfographicV1Renderer } from './InfographicV1Renderer'
import type { CreationArtifact } from '@/lib/types/creation'

const { toPng, toSvg, triggerBrowserDownload } = vi.hoisted(() => ({
  toPng: vi.fn().mockResolvedValue('data:image/png;base64,AAAA'),
  toSvg: vi.fn().mockResolvedValue('data:image/svg+xml,AAAA'),
  triggerBrowserDownload: vi.fn(),
}))
vi.mock('html-to-image', () => ({ toPng, toSvg }))
vi.mock('@/lib/api/creation', () => ({ triggerBrowserDownload }))

// jsdom lacks fetch(dataUrl).blob(); stub it for the export path.
beforeEach(() => {
  toPng.mockClear()
  toSvg.mockClear()
  triggerBrowserDownload.mockClear()
  global.fetch = vi
    .fn()
    .mockResolvedValue({ blob: () => Promise.resolve(new Blob(['x'])) }) as unknown as typeof fetch
})

function makeArtifact(overrides: Partial<CreationArtifact> = {}): CreationArtifact {
  return {
    id: 'creation_artifact:3',
    creator_key: 'infographics',
    schema_id: 'infographic.v1',
    name: 'Climate',
    status: 'completed',
    data: {
      title: 'Climate Snapshot',
      subtitle: 'Key figures',
      blocks: [
        { type: 'stat', value: '1.5°C', label: 'Target', icon: 'thermometer', description: 'limit' },
        { type: 'text', heading: 'Why', body: 'It matters.' },
        { type: 'list', heading: 'Drivers', items: ['energy', 'transport'] },
        { type: 'quote', text: 'Act now.', attribution: 'IPCC' },
      ],
    },
    files: [],
    config: { theme: 'light' },
    warnings: [],
    errors: [],
    ...overrides,
  }
}

describe('InfographicV1Renderer', () => {
  it('renders title, subtitle and all block types', () => {
    render(<InfographicV1Renderer artifact={makeArtifact()} />)
    expect(screen.getByText('Climate Snapshot')).toBeDefined()
    expect(screen.getByText('Key figures')).toBeDefined()
    expect(screen.getByText('1.5°C')).toBeDefined() // stat
    expect(screen.getByText('It matters.')).toBeDefined() // text
    expect(screen.getByText('energy')).toBeDefined() // list item
    expect(screen.getByText('Act now.')).toBeDefined() // quote
  })

  it('offers PNG and SVG export', () => {
    render(<InfographicV1Renderer artifact={makeArtifact()} />)
    expect(screen.getByText('creation.infographics.exportPng')).toBeDefined()
    expect(screen.getByText('creation.infographics.exportSvg')).toBeDefined()
  })

  it('exports PNG via html-to-image', async () => {
    render(<InfographicV1Renderer artifact={makeArtifact()} />)
    fireEvent.click(screen.getByText('creation.infographics.exportPng'))
    await waitFor(() => expect(toPng).toHaveBeenCalled())
    await waitFor(() => expect(triggerBrowserDownload).toHaveBeenCalled())
    expect(triggerBrowserDownload.mock.calls[0][1].endsWith('.png')).toBe(true)
  })

  it('shows an error for invalid data', () => {
    render(<InfographicV1Renderer artifact={makeArtifact({ data: { nope: true } })} />)
    expect(screen.getByText('creation.invalidArtifactData')).toBeDefined()
  })
})
