import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'

import { ChartSpecRenderer } from './ChartSpecRenderer'
import type { CreationArtifact } from '@/lib/types/creation'

// --- mocks ------------------------------------------------------------------
// Use a real class so `new Chart()` and its methods behave normally; each method
// delegates to a spy. (A vi.fn() with `new` doesn't reliably adopt a returned object.)
const { ChartCtor, chartOptions, chartRender, triggerBrowserDownload, FakeChart } =
  vi.hoisted(() => {
    const ChartCtor = vi.fn()
    const chartOptions = vi.fn()
    const chartRender = vi.fn()
    const chartDestroy = vi.fn()
    class FakeChart {
      constructor(opts: unknown) {
        ChartCtor(opts)
      }
      options(spec: unknown) {
        chartOptions(spec)
        return this
      }
      render() {
        chartRender()
        return this
      }
      destroy() {
        chartDestroy()
      }
    }
    return { ChartCtor, chartOptions, chartRender, triggerBrowserDownload: vi.fn(), FakeChart }
  })
vi.mock('@antv/g2', () => ({ Chart: FakeChart }))
vi.mock('@/lib/api/creation', () => ({ triggerBrowserDownload }))

function makeArtifact(overrides: Partial<CreationArtifact> = {}): CreationArtifact {
  return {
    id: 'creation_artifact:2',
    creator_key: 'infographics',
    schema_id: 'chart_spec.v1',
    name: 'Sales',
    status: 'completed',
    data: {
      title: 'Quarterly Sales',
      specs: [
        { type: 'interval', data: [{ category: 'A', value: 3 }], encode: { x: 'category', y: 'value' } },
        { type: 'line', data: [{ category: 'A', value: 1 }], encode: { x: 'category', y: 'value' } },
      ],
    },
    files: [],
    config: {},
    warnings: [],
    errors: [],
    ...overrides,
  }
}

describe('ChartSpecRenderer', () => {
  beforeEach(() => {
    ChartCtor.mockClear()
    chartOptions.mockClear()
    chartRender.mockClear()
    triggerBrowserDownload.mockClear()
  })

  it('renders the title and PNG + SVG export buttons per spec', () => {
    render(<ChartSpecRenderer artifact={makeArtifact()} />)
    expect(screen.getByText('Quarterly Sales')).toBeDefined()
    // one PNG + one SVG export button per chart spec (2 specs)
    expect(screen.getAllByText('creation.infographics.exportPng')).toHaveLength(2)
    expect(screen.getAllByText('creation.infographics.exportSvg')).toHaveLength(2)
  })

  it('renders one chart container per spec', () => {
    render(<ChartSpecRenderer artifact={makeArtifact()} />)
    expect(screen.getAllByTestId('chart-canvas')).toHaveLength(2)
  })

  it('instantiates AntV charts via dynamic import', async () => {
    render(<ChartSpecRenderer artifact={makeArtifact()} />)
    // @antv/g2 is loaded lazily inside an effect; wait for the render call itself.
    await waitFor(() => expect(chartRender).toHaveBeenCalled(), { timeout: 3000 })
    expect(ChartCtor).toHaveBeenCalled()
  })

  it('does not crash exporting when no canvas is present', () => {
    render(<ChartSpecRenderer artifact={makeArtifact()} />)
    fireEvent.click(screen.getAllByText('creation.infographics.exportPng')[0])
    // jsdom has no real canvas; export is a no-op rather than a throw
    expect(triggerBrowserDownload).not.toHaveBeenCalled()
  })

  it('shows an error for invalid artifact data', () => {
    render(<ChartSpecRenderer artifact={makeArtifact({ data: { specs: 'not-an-array' } })} />)
    expect(screen.getByText('creation.invalidArtifactData')).toBeDefined()
  })
})
