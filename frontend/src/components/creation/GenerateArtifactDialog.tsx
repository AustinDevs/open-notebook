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
import { CreatorManifest } from '@/lib/types/creation'

interface Props {
  manifest: CreatorManifest
  notebookId?: string
  open: boolean
  onOpenChange: (open: boolean) => void
}

type ModelKind = 'language' | 'embedding' | 'speech_to_text' | 'text_to_speech'
const MODEL_KINDS: ModelKind[] = ['language', 'embedding', 'speech_to_text', 'text_to_speech']

interface SchemaProp {
  type?: string
  title?: string
  default?: unknown
  anyOf?: { type?: string }[]
}

function propType(p: SchemaProp): string {
  if (p.type) return p.type
  const t = p.anyOf?.find(v => v.type && v.type !== 'null')?.type
  return t || 'string'
}

export function GenerateArtifactDialog({ manifest, notebookId, open, onOpenChange }: Props) {
  const { t } = useTranslation()
  const { data: notebooks } = useNotebooks()
  const generate = useGenerateCreationArtifact(manifest.key, notebookId)

  const [name, setName] = useState('')
  const [selectedNotebook, setSelectedNotebook] = useState<string | undefined>(notebookId)
  const [models, setModels] = useState<Record<string, string>>({})
  const [config, setConfig] = useState<Record<string, unknown>>({})

  const properties = useMemo(
    () => (manifest.config_schema?.properties ?? {}) as Record<string, SchemaProp>,
    [manifest.config_schema]
  )

  useEffect(() => {
    if (!open) {
      setName('')
      setModels({})
      setConfig({})
      setSelectedNotebook(notebookId)
    }
  }, [open, notebookId])

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
              onChange={e => setName(e.target.value)}
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
                          : type === 'number'
                            ? parseFloat(raw)
                            : raw,
                    }))
                  }}
                />
              </div>
            )
          })}
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
