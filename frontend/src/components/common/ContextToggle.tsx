'use client'

import { EyeOff, Eye } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'
import { ContextMode } from '@/app/(dashboard)/notebooks/[id]/page'
import { useTranslation } from '@/lib/hooks/use-translation'

interface ContextToggleProps {
  mode: ContextMode
  hasInsights?: boolean
  onChange: (mode: ContextMode) => void
  className?: string
}

export function ContextToggle({ mode, onChange, className }: ContextToggleProps) {
  const { t } = useTranslation()

  const isOn = mode === 'on'

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation()
    onChange(isOn ? 'off' : 'on')
  }

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ghost"
            size="sm"
            className={cn(
              'h-8 w-8 p-0 transition-colors',
              isOn ? 'hover:bg-primary/10' : 'hover:bg-muted',
              className
            )}
            onClick={handleClick}
          >
            {isOn ? (
              <Eye className="h-4 w-4 text-primary" />
            ) : (
              <EyeOff className="h-4 w-4 text-muted-foreground" />
            )}
          </Button>
        </TooltipTrigger>
        <TooltipContent>
          <p className="text-xs">
            {isOn ? t.common.contextModes.on : t.common.contextModes.off}
          </p>
          <p className="text-[10px] text-muted-foreground mt-1">
            {t.common.contextModes.clickToToggle}
          </p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
