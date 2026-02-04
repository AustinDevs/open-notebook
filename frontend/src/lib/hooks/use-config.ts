import { useEffect, useState } from 'react'
import { getConfig } from '@/lib/config'
import { AppConfig } from '@/lib/types/config'

/**
 * Hook to access application configuration.
 *
 * Returns the cached config if available, otherwise fetches it.
 * Useful for accessing runtime configuration like authMode.
 */
export function useConfig() {
  const [config, setConfig] = useState<AppConfig | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    let mounted = true

    getConfig()
      .then((cfg) => {
        if (mounted) {
          setConfig(cfg)
          setIsLoading(false)
        }
      })
      .catch(() => {
        if (mounted) {
          setIsLoading(false)
        }
      })

    return () => {
      mounted = false
    }
  }, [])

  return { config, isLoading }
}
