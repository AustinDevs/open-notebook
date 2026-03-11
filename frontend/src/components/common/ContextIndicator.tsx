'use client'

import { FileText, StickyNote } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Tooltip, TooltipTrigger, TooltipContent } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'
import { useTranslation } from '@/lib/hooks/use-translation'

interface ContextIndicatorProps {
  sourcesCount: number
  notesCount: number
  className?: string
}

export function ContextIndicator({
  sourcesCount,
  notesCount,
  className
}: ContextIndicatorProps) {
  const { t } = useTranslation()
  const hasContext = sourcesCount > 0 || notesCount > 0

  if (!hasContext) {
    return (
      <div className={cn('flex-shrink-0 text-xs text-muted-foreground py-2 px-3 border-t', className)}>
        {t.common.contextModes.noContext}
      </div>
    )
  }

  return (
    <div className={cn('flex-shrink-0 flex items-center gap-2 py-2 px-3 border-t bg-muted/30', className)}>
      <span className="text-xs font-medium text-muted-foreground">{t.common.context}:</span>

      <div className="flex items-center gap-1.5">
        {sourcesCount > 0 && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge variant="outline" className="text-xs flex items-center gap-1 px-1.5 py-0.5 text-primary border-primary/50 cursor-default">
                <FileText className="h-3 w-3" />
                <span>{sourcesCount}</span>
              </Badge>
            </TooltipTrigger>
            <TooltipContent>
              <p>{sourcesCount} {sourcesCount === 1 ? t.common.source : t.navigation.sources} {t.common.contextModes.inScope}</p>
            </TooltipContent>
          </Tooltip>
        )}

        {notesCount > 0 && (
          <>
            {sourcesCount > 0 && (
              <span className="text-muted-foreground">,</span>
            )}
            <Tooltip>
              <TooltipTrigger asChild>
                <Badge variant="outline" className="text-xs flex items-center gap-1 px-1.5 py-0.5 text-primary border-primary/50 cursor-default">
                  <StickyNote className="h-3 w-3" />
                  <span>{notesCount}</span>
                </Badge>
              </TooltipTrigger>
              <TooltipContent>
                <p>{notesCount} {notesCount === 1 ? t.common.note : t.common.notes} {t.common.contextModes.inScope}</p>
              </TooltipContent>
            </Tooltip>
          </>
        )}
      </div>
    </div>
  )
}
