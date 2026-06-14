'use client'

import { ComponentType } from 'react'

import { CreationArtifact } from '@/lib/types/creation'
import { FallbackRenderer } from './FallbackRenderer'
import { FlashcardsV1Renderer } from './FlashcardsV1Renderer'
import { ChartSpecV1Renderer } from './ChartSpecV1Renderer'
import { InfographicV1Renderer } from './InfographicV1Renderer'
import { MindmapV1Renderer } from './MindmapV1Renderer'
import { TextbookV1Renderer } from './TextbookV1Renderer'

export type ArtifactRenderer = ComponentType<{ artifact: CreationArtifact }>

/**
 * Maps a versioned artifact `schema_id` to its React renderer. Reusing an
 * existing schema in a new creator needs zero changes here; a new schema needs
 * one new entry. Unknown ids fall back to a safe raw-JSON view.
 */
export const artifactRenderers: Record<string, ArtifactRenderer> = {
  'flashcards.v1': FlashcardsV1Renderer,
  'chart_spec.v1': ChartSpecV1Renderer,
  'infographic.v1': InfographicV1Renderer,
  'mindmap.v1': MindmapV1Renderer,
  'textbook.v1': TextbookV1Renderer,
}

export function getRenderer(schemaId?: string | null): ArtifactRenderer {
  if (schemaId && artifactRenderers[schemaId]) return artifactRenderers[schemaId]
  return FallbackRenderer
}
