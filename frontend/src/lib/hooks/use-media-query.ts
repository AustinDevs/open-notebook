'use client'

import { useState, useEffect } from 'react'

/**
 * Hook to detect if viewport matches a media query.
 * Returns false during SSR to avoid hydration mismatches.
 */
export function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState(false)

  useEffect(() => {
    const mediaQuery = window.matchMedia(query)
    setMatches(mediaQuery.matches)

    const handler = (event: MediaQueryListEvent) => {
      setMatches(event.matches)
    }

    mediaQuery.addEventListener('change', handler)
    return () => mediaQuery.removeEventListener('change', handler)
  }, [query])

  return matches
}

/**
 * Returns true if viewport is >= 1024px (Tailwind's 'lg' breakpoint)
 */
export function useIsDesktop(): boolean {
  return useMediaQuery('(min-width: 1024px)')
}

/**
 * Returns true if viewport is >= 1280px (Tailwind's 'xl' breakpoint).
 * The notebook's 3-column layout (sources + chat + creations) needs more room
 * than 'lg' before the columns can sit side by side without squishing chat.
 */
export function useIsWideDesktop(): boolean {
  return useMediaQuery('(min-width: 1280px)')
}
