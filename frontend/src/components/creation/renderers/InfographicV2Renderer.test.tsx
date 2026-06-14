import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'

import { InfographicV2Renderer } from './InfographicV2Renderer'
import type { CreationArtifact } from '@/lib/types/creation'

// --- mocks ------------------------------------------------------------------
// Real class so `new Infographic()` and its methods behave normally; each method
// delegates to a spy. (A vi.fn() with `new` doesn't reliably adopt a returned object.)
const { InfographicCtor, igRender, igToDataURL, triggerBrowserDownload, FakeInfographic } =
  vi.hoisted(() => {
    const InfographicCtor = vi.fn()
    const igRender = vi.fn()
    const igDestroy = vi.fn()
    const igToDataURL = vi.fn(async (_opts: unknown) => 'data:image/png;base64,AAAA')
    class FakeInfographic {
      constructor(opts: unknown) {
        InfographicCtor(opts)
      }
      render(spec: string) {
        igRender(spec)
      }
      toDataURL(o: unknown) {
        return igToDataURL(o)
      }
      destroy() {
        igDestroy()
      }
    }
    return { InfographicCtor, igRender, igToDataURL, triggerBrowserDownload: vi.fn(), FakeInfographic }
  })
vi.mock('@antv/infographic', () => ({ Infographic: FakeInfographic }))
vi.mock('@/lib/api/creation', () => ({ triggerBrowserDownload }))

const SPEC = 'infographic list-grid-simple\ndata\n  lists\n    - label A\n    - label B'

function makeArtifact(overrides: Partial<CreationArtifact> = {}): CreationArtifact {
  return {
    id: 'creation_artifact:3',
    creator_key: 'infographics',
    schema_id: 'infographic.v2',
    name: 'Growth',
    status: 'completed',
    data: { title: 'Product Growth', library: 'antv-infographic', spec: SPEC },
    files: [],
    config: {},
    warnings: [],
    errors: [],
    ...overrides,
  }
}

describe('InfographicV2Renderer', () => {
  beforeEach(() => {
    InfographicCtor.mockClear()
    igRender.mockClear()
    igToDataURL.mockClear()
    triggerBrowserDownload.mockClear()
    // jsdom lacks fetch used by the export path; stub a minimal blob response.
    vi.stubGlobal('fetch', vi.fn(async () => ({ blob: async () => new Blob(['x']) })))
  })

  it('renders the title and PNG + SVG export buttons', () => {
    render(<InfographicV2Renderer artifact={makeArtifact()} />)
    expect(screen.getByText('Product Growth')).toBeDefined()
    expect(screen.getAllByText('creation.infographics.exportPng')).toHaveLength(1)
    expect(screen.getAllByText('creation.infographics.exportSvg')).toHaveLength(1)
  })

  it('instantiates the AntV infographic via dynamic import and renders the spec', async () => {
    render(<InfographicV2Renderer artifact={makeArtifact()} />)
    await waitFor(() => expect(igRender).toHaveBeenCalled(), { timeout: 3000 })
    expect(InfographicCtor).toHaveBeenCalled()
    expect(igRender).toHaveBeenCalledWith(SPEC)
  })

  it('exports a PNG via the engine toDataURL', async () => {
    render(<InfographicV2Renderer artifact={makeArtifact()} />)
    await waitFor(() => expect(igRender).toHaveBeenCalled(), { timeout: 3000 })
    fireEvent.click(screen.getByText('creation.infographics.exportPng'))
    await waitFor(() => expect(triggerBrowserDownload).toHaveBeenCalled(), { timeout: 3000 })
    expect(igToDataURL).toHaveBeenCalledWith({ type: 'png', dpr: 2 })
  })

  it('shows an error for invalid artifact data', () => {
    render(<InfographicV2Renderer artifact={makeArtifact({ data: { spec: 123 } })} />)
    expect(screen.getByText('creation.invalidArtifactData')).toBeDefined()
  })
})
