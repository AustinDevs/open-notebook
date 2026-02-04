'use client'

import { useEffect } from 'react'
import { usePathname } from 'next/navigation'

/**
 * Syncs navigation changes to parent window when embedded in an iframe.
 * The parent window (notebooker) listens for 'open-notebook-navigation' messages
 * and updates its URL to preserve the iframe path on refresh.
 */
export function useParentNavigationSync() {
  const pathname = usePathname()

  useEffect(() => {
    // Only send messages if we're in an iframe
    if (window.parent === window) {
      return
    }

    // Remove basePath prefix if present (parent expects relative path)
    const basePath = process.env.NEXT_PUBLIC_BASE_PATH || ''
    let relativePath = pathname
    if (basePath && pathname.startsWith(basePath)) {
      relativePath = pathname.slice(basePath.length) || '/'
    }

    // Remove leading slash for consistency
    const path = relativePath.replace(/^\//, '') || 'notebooks'

    // Send navigation message to parent
    window.parent.postMessage(
      { type: 'open-notebook-navigation', path },
      '*'
    )
  }, [pathname])
}
