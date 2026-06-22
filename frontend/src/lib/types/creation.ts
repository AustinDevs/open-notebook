// Host-side types for the Creation plugin system. Artifact `data` shapes are
// validated/typed via the generated contract in `creation.generated.ts`.

export interface CreationModelRole {
  key: string
  kind: 'language' | 'text_to_speech' | 'speech_to_text' | 'embedding' | string
  requires: string[]
  min_context_window?: number | null
  provider_allowlist?: string[] | null
  params_schema?: Record<string, unknown> | null
  required: boolean
  description?: string
}

export interface CreatorManifest {
  key: string
  name: string
  version: string
  description?: string
  emits: string[]
  model_roles: CreationModelRole[]
  config_schema: Record<string, unknown>
  icon?: string | null
  has_custom_form?: boolean
  // True when the plugin ships its own self-contained HTML view bundle, served
  // from `/creation/creators/{key}/view` and rendered by `PluginViewRenderer`.
  has_view?: boolean
  available: boolean
  error?: string | null
}

export interface CreatorsResponse {
  creators: CreatorManifest[]
  registry_digest: string
}

export type CreationStatus =
  | 'submitted'
  | 'running'
  | 'completed'
  | 'partial'
  | 'failed'

export interface CreationArtifactFile {
  filename: string
  content_type: string
  path: string
  label?: string | null
}

export interface CreationArtifactError {
  phase: string
  message: string
  retryable?: boolean
  details?: Record<string, unknown>
}

export interface CreationArtifact {
  id: string
  notebook_id?: string | null
  creator_key: string
  creator_version?: string | null
  sdk_version?: string | null
  schema_id?: string | null
  name: string
  status: CreationStatus
  data: Record<string, unknown>
  files: CreationArtifactFile[]
  config: Record<string, unknown>
  warnings: string[]
  errors: CreationArtifactError[]
  user_message?: string | null
  error_message?: string | null
  command?: string | null
  job_status?: string | null
}

export interface GenerateCreationRequest {
  creator_key: string
  name: string
  config?: Record<string, unknown>
  models?: Record<string, string>
  notebook_id?: string
  content?: string
  language?: string
  instructions?: string
}

export interface GenerateCreationResponse {
  job_id: string
  artifact_id: string
  status: string
}

export const ACTIVE_CREATION_STATUSES: CreationStatus[] = ['submitted', 'running']
