'use client'

import { useEffect, useMemo, useState } from 'react'

import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Label } from '@/components/ui/label'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { ModelSelector } from '@/components/common/ModelSelector'
import { useTranslation } from '@/lib/hooks/use-translation'
import { useNotebooks } from '@/lib/hooks/use-notebooks'
import { useGenerateCreationArtifact } from '@/lib/hooks/use-creation'
import { useModelDefaults } from '@/lib/hooks/use-models'
import { CreatorManifest } from '@/lib/types/creation'
import { ModelDefaults } from '@/lib/types/models'

interface Props {
  manifest: CreatorManifest
  notebookId?: string
  open: boolean
  onOpenChange: (open: boolean) => void
}

type ModelKind = 'language' | 'embedding' | 'speech_to_text' | 'text_to_speech' | 'image'
const MODEL_KINDS: ModelKind[] = ['language', 'embedding', 'speech_to_text', 'text_to_speech', 'image']

interface SchemaProp {
  type?: string
  title?: string
  default?: unknown
  anyOf?: { type?: string }[]
  enum?: (string | number)[]
  // Optional help link (from a Pydantic field's json_schema_extra). Rendered as
  // a "learn more" link beside the field, opening in a new tab.
  'x-help-url'?: string
  'x-help-label'?: string
}

function propType(p: SchemaProp): string {
  if (p.type) return p.type
  const t = p.anyOf?.find(v => v.type && v.type !== 'null')?.type
  return t || 'string'
}

// Turn an enum value into a human label, e.g. "classicDark" -> "Classic Dark".
function prettyOption(value: string | number): string {
  return String(value)
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
}

// The user's configured default Model id for a role kind, mirroring the
// backend's per-kind fallback (see api/creation_service.py _DEFAULT_BY_KIND).
function defaultModelForKind(kind: ModelKind, d?: ModelDefaults | null): string {
  if (!d) return ''
  switch (kind) {
    case 'language':
      return d.default_transformation_model || d.default_chat_model || ''
    case 'embedding':
      return d.default_embedding_model || ''
    case 'text_to_speech':
      return d.default_text_to_speech_model || ''
    case 'speech_to_text':
      return d.default_speech_to_text_model || ''
    default:
      return ''
  }
}

// Creator names are plural ("Infographics", "Flashcards", …); a single
// artifact's default name reads better singular. Crude but covers our names;
// leaves "ss" words (e.g. "Class") and non-plural names untouched.
function singularize(name: string): string {
  const n = name.trim()
  if (n.length > 1 && /s$/i.test(n) && !/ss$/i.test(n)) return n.slice(0, -1)
  return n
}

export function GenerateArtifactDialog({ manifest, notebookId, open, onOpenChange }: Props) {
  const { t } = useTranslation()
  const { data: notebooks } = useNotebooks()
  const generate = useGenerateCreationArtifact(manifest.key, notebookId)
  const { data: modelDefaults } = useModelDefaults()

  const [name, setName] = useState('')
  const [selectedNotebook, setSelectedNotebook] = useState<string | undefined>(notebookId)
  const [models, setModels] = useState<Record<string, string>>({})
  const [config, setConfig] = useState<Record<string, unknown>>({})
  const [instructions, setInstructions] = useState('')
  // Whether the name still tracks the auto-generated default (vs. user-edited).
  const [nameAuto, setNameAuto] = useState(true)

  const properties = useMemo(
    () => (manifest.config_schema?.properties ?? {}) as Record<string, SchemaProp>,
    [manifest.config_schema]
  )

  const notebookName = useMemo(
    () => notebooks?.find(nb => nb.id === selectedNotebook)?.name,
    [notebooks, selectedNotebook]
  )
  // Default artifact name: singular creator name, prefixed with the selected
  // notebook (e.g. "2026 Mindanao earthquake Infographic").
  const autoName = useMemo(() => {
    const singular = singularize(manifest.name)
    return notebookName ? `${notebookName} ${singular}` : singular
  }, [manifest.name, notebookName])

  useEffect(() => {
    if (!open) {
      setName('')
      setNameAuto(true)
      setModels({})
      setConfig({})
      setInstructions('')
      setSelectedNotebook(notebookId)
      return
    }
    // On open, pre-fill sensible defaults (the user can still change them): the
    // name (handled by the autoName effect below) and, per model role, the
    // user's configured default model so they don't have to pick one.
    setSelectedNotebook(notebookId)
    setModels(prev => {
      const next = { ...prev }
      for (const role of manifest.model_roles) {
        if (next[role.key]) continue
        const kind = (
          MODEL_KINDS.includes(role.kind as ModelKind) ? role.kind : 'language'
        ) as ModelKind
        const def = defaultModelForKind(kind, modelDefaults)
        if (def) next[role.key] = def
      }
      return next
    })
  }, [open, notebookId, manifest, modelDefaults])

  // Keep the name on the auto-generated default until the user edits it, so it
  // reflects the selected notebook (and updates if the notebook changes).
  useEffect(() => {
    if (open && nameAuto) setName(autoName)
  }, [open, nameAuto, autoName])

  const canSubmit =
    name.trim().length > 0 &&
    Boolean(selectedNotebook) &&
    manifest.model_roles.every(r => !r.required || models[r.key])

  const onSubmit = async () => {
    await generate.mutateAsync({
      creator_key: manifest.key,
      name: name.trim(),
      config,
      models,
      notebook_id: selectedNotebook,
      instructions: instructions.trim() || undefined,
    })
    onOpenChange(false)
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[520px]">
        <DialogHeader>
          <DialogTitle>
            {t('creation.generateTitle').replace('{name}', manifest.name)}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          <div className="space-y-2">
            <Label>{t('creation.nameLabel')} *</Label>
            <Input
              value={name}
              onChange={e => {
                setName(e.target.value)
                setNameAuto(false)
              }}
              placeholder={manifest.name}
            />
          </div>

          <div className="space-y-2">
            <Label>{t('creation.notebookLabel')} *</Label>
            <Select value={selectedNotebook} onValueChange={setSelectedNotebook}>
              <SelectTrigger>
                <SelectValue placeholder={t('creation.selectNotebook')} />
              </SelectTrigger>
              <SelectContent>
                {(notebooks ?? []).map(nb => (
                  <SelectItem key={nb.id} value={nb.id}>
                    {nb.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {manifest.model_roles.map(role => (
            <ModelSelector
              key={role.key}
              label={`${t('creation.modelFor')} ${role.description || role.key}`}
              modelType={
                (MODEL_KINDS.includes(role.kind as ModelKind)
                  ? role.kind
                  : 'language') as ModelKind
              }
              value={models[role.key] ?? ''}
              onChange={v => setModels(m => ({ ...m, [role.key]: v }))}
              placeholder={t('creation.useDefaultModel')}
            />
          ))}

          {Object.entries(properties).map(([key, prop]) => {
            const type = propType(prop)
            const labelText = prop.title || key
            if (prop.enum && prop.enum.length > 0) {
              const current = String(config[key] ?? prop.default ?? prop.enum[0])
              return (
                <div key={key} className="space-y-2">
                  <Label>{labelText}</Label>
                  <Select
                    value={current}
                    onValueChange={v => setConfig(c => ({ ...c, [key]: v }))}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {prop.enum.map(opt => (
                        <SelectItem key={String(opt)} value={String(opt)}>
                          {prettyOption(opt)}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {prop['x-help-url'] && (
                    <a
                      href={prop['x-help-url']}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-xs text-muted-foreground underline"
                    >
                      {prop['x-help-label'] || 'Learn more'} ↗
                    </a>
                  )}
                </div>
              )
            }
            if (type === 'boolean') {
              return (
                <label key={key} className="flex items-center gap-2 text-sm">
                  <Checkbox
                    checked={Boolean(config[key] ?? prop.default)}
                    onCheckedChange={v => setConfig(c => ({ ...c, [key]: Boolean(v) }))}
                  />
                  {labelText}
                </label>
              )
            }
            return (
              <div key={key} className="space-y-2">
                <Label>{labelText}</Label>
                <Input
                  type={type === 'integer' || type === 'number' ? 'number' : 'text'}
                  defaultValue={prop.default != null ? String(prop.default) : ''}
                  onChange={e => {
                    const raw = e.target.value
                    setConfig(c => ({
                      ...c,
                      [key]:
                        type === 'integer'
                          ? parseInt(raw, 10)
                          : parseFloat === undefined
                            ? raw
                            : type === 'number'
                            ? parseFloat(raw)
                            : raw,
                    }))
                  }}
                />
              </div>
            )
          })}

          <div className="space-y-2">
            <Label>{t('creation.instructionsLabel')}</Label>
            <Textarea
              value={instructions}
              onChange={e => setInstructions(e.target.value)}
              placeholder={t('creation.instructionsPlaceholder')}
              rows={3}
            />
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            {t('common.cancel')}
          </Button>
          <Button onClick={onSubmit} disabled={!canSubmit || generate.isPending}>
            {generate.isPending ? t('creation.generating') : t('creation.generate')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
