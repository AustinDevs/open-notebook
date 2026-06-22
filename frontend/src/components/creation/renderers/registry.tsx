'use client'

import { ComponentType } from 'react'

import { CreationArtifact } from '@/lib/types/creation'
import { FallbackRenderer } from './FallbackRenderer'
import { FlashcardsV1Renderer } from './FlashcardsV1Renderer'
import { ChartSpecV1Renderer } from './ChartSpecV1Renderer'
import { InfographicV1Renderer } from './InfographicV1Renderer'
import { InfographicV2Renderer } from './InfographicV2Renderer'
import { MindmapV1Renderer } from './MindmapV1Renderer'

export type ArtifactRenderer = ComponentType<{ artifact: CreationArtifact }>

/**
 * Maps a versioned artifact `schema_id` to its core React renderer. This is the
 * legacy/fallback path: creators that ship their own view bundle (or emit a
 * self-contained HTML file, like textbook) are rendered by `PluginViewRenderer`
 * instead and need no entry here. Unknown ids fall back to a raw-JSON view.
 */
export const artifactRenderers: Record<string, ArtifactRenderer> = {
  'flashcards.v1': FlashcardsV1Renderer,
  'chart_spec.v1': ChartSpecV1Renderer,
  'infographic.v1': InfographicV1Renderer,
  'infographic.v2': InfographicV2Renderer,
  'mindmap.v1': MindmapV1Renderer,
}

export function getRenderer(schemaId?: string | null): ArtifactRenderer {
  if (schemaId && artifactRenderers[schemaId]) return artifactRenderers[schemaId]
  return FallbackRenderer
}
