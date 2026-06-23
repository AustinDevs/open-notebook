'use client'

import { ComponentType } from 'react'

import { CreationArtifact } from '@/lib/types/creation'
import { FallbackRenderer } from './FallbackRenderer'

export type ArtifactRenderer = ComponentType<{ artifact: CreationArtifact }>

/**
 * Core schema renderers used to live here, but every creator now owns its UI —
 * either a self-contained view bundle (`has_view`) or an emitted HTML file — both
 * rendered by {@link PluginViewRenderer}. This registry is only the safety net for
 * an artifact that has neither: it renders a raw-JSON {@link FallbackRenderer}. Add
 * an entry here only if you ever need a host-side renderer for a bundle-less schema.
 */
export const artifactRenderers: Record<string, ArtifactRenderer> = {}

export function getRenderer(schemaId?: string | null): ArtifactRenderer {
  if (schemaId && artifactRenderers[schemaId]) return artifactRenderers[schemaId]
  return FallbackRenderer
}
