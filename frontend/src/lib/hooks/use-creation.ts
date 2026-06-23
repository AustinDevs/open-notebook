import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { creationApi } from '@/lib/api/creation'
import { QUERY_KEYS } from '@/lib/api/query-client'
import { useToast } from '@/lib/hooks/use-toast'
import { useTranslation } from '@/lib/hooks/use-translation'
import { getApiErrorKey } from '@/lib/utils/error-handler'
import {
  ACTIVE_CREATION_STATUSES,
  CreationArtifact,
  GenerateCreationRequest,
} from '@/lib/types/creation'

export function useCreators() {
  const query = useQuery({
    queryKey: QUERY_KEYS.creators,
    queryFn: creationApi.listCreators,
    staleTime: Infinity,
  })
  return {
    ...query,
    creators: query.data?.creators ?? [],
    registryDigest: query.data?.registry_digest,
  }
}

function hasActive(items: CreationArtifact[]) {
  return items.some(a => ACTIVE_CREATION_STATUSES.includes(a.status))
}

export function useCreationArtifacts(creatorKey?: string, notebookId?: string) {
  const query = useQuery({
    queryKey: QUERY_KEYS.creationArtifacts(creatorKey, notebookId),
    queryFn: () =>
      creationApi.listArtifacts({ creator_key: creatorKey, notebook_id: notebookId }),
    enabled: Boolean(creatorKey),
    refetchInterval: query =>
      hasActive((query.state.data as CreationArtifact[] | undefined) ?? [])
        ? 4000
        : false,
  })
  return { ...query, artifacts: query.data ?? [] }
}

export function useGenerateCreationArtifact(creatorKey?: string, notebookId?: string) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const { t } = useTranslation()

  return useMutation({
    mutationFn: async (payload: GenerateCreationRequest) => {
      const res = await creationApi.generate(payload)
      // Fire-and-poll: resolve the list once the job finishes.
      void creationApi
        .waitForCommand(res.job_id, { maxAttempts: 150, intervalMs: 3000 })
        .then(() =>
          queryClient.invalidateQueries({
            queryKey: QUERY_KEYS.creationArtifacts(creatorKey, notebookId),
          })
        )
      return res
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: QUERY_KEYS.creationArtifacts(creatorKey, notebookId),
      })
      toast({ title: t('creation.generationStarted') })
    },
    onError: (error: unknown) => {
      toast({
        title: t('creation.failedToGenerate'),
        description: getApiErrorKey(error, t('common.error')),
        variant: 'destructive',
      })
    },
  })
}

export function useDeleteCreationArtifact(creatorKey?: string, notebookId?: string) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const { t } = useTranslation()
  return useMutation({
    mutationFn: (id: string) => creationApi.deleteArtifact(id),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: QUERY_KEYS.creationArtifacts(creatorKey, notebookId),
      })
      toast({ title: t('creation.artifactDeleted') })
    },
    onError: (error: unknown) => {
      toast({
        title: t('common.error'),
        description: getApiErrorKey(error, t('common.error')),
        variant: 'destructive',
      })
    },
  })
}