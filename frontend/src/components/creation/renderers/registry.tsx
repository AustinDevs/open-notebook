'use client'

import { ComponentType } from 'react'

import { CreationArtifact } from '@/lib/types/creation'
import { FallbackRenderer } from './FallbackRenderer'
import { FlashcardsRenderer } from './FlashcardsRenderer'
import { ChartSpecRenderer } from './ChartSpecRenderer'

export type ArtifactRenderer = ComponentType<{ artifact: CreationArtifact }>

/**
 * Maps a versioned artifact `schema_id` to its React renderer. Reusing an
 * existing schema in a new creator needs zero changes here; a new schema needs
 * one new entry. Unknown ids fall back to a safe raw-JSON view.
 */
export const artifactRenderers: Record<string, ArtifactRenderer> = {
  'flashcards.v1': FlashcardsRenderer,
  'chart_spec.v1': ChartSpecRenderer,
}

export function getRenderer(schemaId?: string | null): ArtifactRenderer {
  if (schemaId && artifactRenderers[schemaId]) return artifactRenderers[schemaId]
  return FallbackRenderer
}
