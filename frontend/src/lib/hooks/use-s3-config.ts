import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import {
  s3ConfigApi,
  S3ConfigRequest,
  S3Config,
  S3ConfigStatus,
  S3TestResult,
} from '@/lib/api/s3-config'

const QUERY_KEYS = {
  s3Status: ['s3-config', 'status'],
  s3Config: ['s3-config'],
}

/**
 * Hook to get S3 configuration status
 */
export function useS3Status() {
  return useQuery<S3ConfigStatus>({
    queryKey: QUERY_KEYS.s3Status,
    queryFn: s3ConfigApi.getStatus,
  })
}

/**
 * Hook to get S3 configuration (no secrets)
 */
export function useS3Config() {
  return useQuery<S3Config>({
    queryKey: QUERY_KEYS.s3Config,
    queryFn: s3ConfigApi.getConfig,
  })
}

/**
 * Hook to save S3 configuration
 */
export function useSaveS3Config() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (config: S3ConfigRequest) => s3ConfigApi.saveConfig(config),
    onSuccess: () => {
      // Invalidate both queries to refresh status and config
      queryClient.invalidateQueries({ queryKey: QUERY_KEYS.s3Status })
      queryClient.invalidateQueries({ queryKey: QUERY_KEYS.s3Config })
      toast.success('S3 configuration saved successfully')
    },
    onError: (error: Error) => {
      toast.error(`Failed to save S3 configuration: ${error.message}`)
    },
  })
}

/**
 * Hook to delete S3 configuration
 */
export function useDeleteS3Config() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: () => s3ConfigApi.deleteConfig(),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: QUERY_KEYS.s3Status })
      queryClient.invalidateQueries({ queryKey: QUERY_KEYS.s3Config })
      toast.success('S3 configuration deleted')
    },
    onError: (error: Error) => {
      toast.error(`Failed to delete S3 configuration: ${error.message}`)
    },
  })
}

/**
 * Hook to test S3 connection
 */
export function useTestS3Connection() {
  return useMutation<S3TestResult>({
    mutationFn: () => s3ConfigApi.testConnection(),
    onSuccess: (result) => {
      if (result.success) {
        toast.success(result.message)
      } else {
        toast.error(result.message)
      }
    },
    onError: (error: Error) => {
      toast.error(`Connection test failed: ${error.message}`)
    },
  })
}
