'use client'

import { useEffect, useRef, useState, useCallback } from 'react'
import {
  Mic,
  Square,
  Loader2,
  X,
  Volume2,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { cn } from '@/lib/utils'
import { useInteractivePodcast } from '@/lib/hooks/use-interactive-podcast'
import { useTranslation } from '@/lib/hooks/use-translation'

interface InteractivePodcastPlayerProps {
  episodeId: string
  audioSrc: string
  episodeName?: string
  className?: string
}

const PLAYBACK_RATE_STORAGE_KEY = 'podcast-playback-rate'

export function InteractivePodcastPlayer({
  episodeId,
  audioSrc,
  className,
}: InteractivePodcastPlayerProps) {
  const { t } = useTranslation()
  const audioRef = useRef<HTMLAudioElement>(null)
  const responseAudioRef = useRef<HTMLAudioElement>(null)
  const [currentTime, setCurrentTime] = useState(0)
  const [showResponse, setShowResponse] = useState(false)
  const [playbackRate, setPlaybackRate] = useState(() => {
    if (typeof window !== 'undefined') {
      const stored = localStorage.getItem(PLAYBACK_RATE_STORAGE_KEY)
      return stored ? parseFloat(stored) : 1.0
    }
    return 1.0
  })

  // Getter functions for playback position
  const getCurrentTime = useCallback(() => {
    return audioRef.current?.currentTime ?? 0
  }, [])

  const getTotalDuration = useCallback(() => {
    return audioRef.current?.duration ?? 0
  }, [])

  // Interactive podcast hook
  const {
    state,
    isRecording,
    isProcessing,
    isResponding,
    error,
    lastResponse,
    responseAudioUrl,
    startRecording,
    stopRecording,
    cancelRecording,
    clearResponse,
  } = useInteractivePodcast({
    episodeId,
    onError: (err) => console.error('Interactive podcast error:', err),
    getCurrentTime,
    getTotalDuration,
  })

  // Handle podcast audio events
  useEffect(() => {
    const audio = audioRef.current
    if (!audio) return

    const handleTimeUpdate = () => {
      setCurrentTime(audio.currentTime)
    }

    audio.addEventListener('timeupdate', handleTimeUpdate)

    return () => {
      audio.removeEventListener('timeupdate', handleTimeUpdate)
    }
  }, [])

  // Apply playback rate to audio element
  useEffect(() => {
    if (audioRef.current) {
      audioRef.current.playbackRate = playbackRate
    }
  }, [playbackRate])

  // Handle playback rate change
  const handlePlaybackRateChange = useCallback((value: string) => {
    const rate = parseFloat(value)
    setPlaybackRate(rate)
    localStorage.setItem(PLAYBACK_RATE_STORAGE_KEY, value)
  }, [])

  // Handle response audio events
  useEffect(() => {
    const responseAudio = responseAudioRef.current
    if (!responseAudio || !responseAudioUrl) return

    const handleEnded = () => {
      // When response finishes, clear and resume podcast
      clearResponse()
      setShowResponse(false)
      // Resume podcast playback
      audioRef.current?.play()
    }

    const handleError = () => {
      // When audio fails to load/play, clear and resume podcast
      console.error('Response audio error')
      clearResponse()
      setShowResponse(false)
      audioRef.current?.play()
    }

    responseAudio.addEventListener('ended', handleEnded)
    responseAudio.addEventListener('error', handleError)
    return () => {
      responseAudio.removeEventListener('ended', handleEnded)
      responseAudio.removeEventListener('error', handleError)
    }
  }, [responseAudioUrl, clearResponse])

  // Auto-play response audio when available
  useEffect(() => {
    if (responseAudioUrl && responseAudioRef.current) {
      setShowResponse(true)
      responseAudioRef.current.play().catch((err) => {
        console.error('Failed to play response audio:', err)
        // If audio fails to play, clear and resume podcast
        clearResponse()
        setShowResponse(false)
        audioRef.current?.play()
      })
    }
  }, [responseAudioUrl, clearResponse])

  // Start asking a question
  const handleAskQuestion = useCallback(async () => {
    // Pause the podcast
    audioRef.current?.pause()
    // Start recording
    await startRecording()
  }, [startRecording])

  // Finish asking the question
  const handleFinishQuestion = useCallback(async () => {
    await stopRecording()
  }, [stopRecording])

  // Cancel the question
  const handleCancelQuestion = useCallback(() => {
    cancelRecording()
    // Resume podcast playback
    audioRef.current?.play()
  }, [cancelRecording])

  // Skip the response and resume podcast
  const handleSkipResponse = useCallback(() => {
    // Stop response audio if playing
    if (responseAudioRef.current) {
      responseAudioRef.current.pause()
    }
    clearResponse()
    setShowResponse(false)
    // Resume podcast
    audioRef.current?.play()
  }, [clearResponse])

  // Format time as mm:ss
  const formatTime = (seconds: number) => {
    if (isNaN(seconds)) return '0:00'
    const mins = Math.floor(seconds / 60)
    const secs = Math.floor(seconds % 60)
    return `${mins}:${secs.toString().padStart(2, '0')}`
  }

  return (
    <div className={cn('space-y-2', className)}>
      {/* Main podcast audio player */}
      <audio
        ref={audioRef}
        src={audioSrc}
        controls
        preload="metadata"
        className="w-full"
      />

      {/* Controls row: Speed + Interactive */}
      <div className="flex items-center justify-between gap-3">
        {/* Speed selector */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">{t.podcasts.speed}</span>
          <Select
            value={playbackRate.toString()}
            onValueChange={handlePlaybackRateChange}
          >
            <SelectTrigger className="w-16 !h-7 !py-0 px-2 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="0.5">0.5x</SelectItem>
              <SelectItem value="0.75">0.75x</SelectItem>
              <SelectItem value="1">1x</SelectItem>
              <SelectItem value="1.25">1.25x</SelectItem>
              <SelectItem value="1.5">1.5x</SelectItem>
              <SelectItem value="1.75">1.75x</SelectItem>
              <SelectItem value="2">2x</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Interactive controls */}
        <div className="flex items-center gap-2">
          {/* Idle state: Show "Ask Question" button */}
          {state === 'idle' && (
            <Button
              variant="outline"
              className="h-7 px-2 text-xs"
              onClick={handleAskQuestion}
            >
              <Mic className="mr-1.5 h-3 w-3" />
              {t.podcasts.askQuestion}
            </Button>
          )}

          {/* Recording state */}
          {isRecording && (
            <>
              <div className="flex items-center gap-1.5">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500" />
                </span>
                <span className="text-xs text-red-600 font-medium">
                  {formatTime(currentTime)}
                </span>
              </div>
              <Button
                variant="default"
                className="h-7 px-2 text-xs"
                onClick={handleFinishQuestion}
              >
                <Square className="mr-1 h-2.5 w-2.5 fill-current" />
                {t.podcasts.done}
              </Button>
              <Button
                variant="ghost"
                className="h-7 w-7 p-0"
                onClick={handleCancelQuestion}
              >
                <X className="h-3.5 w-3.5" />
              </Button>
            </>
          )}

          {/* Processing state */}
          {isProcessing && (
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              <span className="text-xs">{t.podcasts.processingQuestion}</span>
            </div>
          )}

          {/* Responding state */}
          {isResponding && (
            <>
              <Volume2 className="h-3.5 w-3.5 text-primary animate-pulse" />
              <span className="text-xs text-primary">
                {t.podcasts.playingResponse}
              </span>
              <Button variant="ghost" className="h-7 px-2 text-xs" onClick={handleSkipResponse}>
                {t.podcasts.skipResponse}
              </Button>
            </>
          )}
        </div>
      </div>

      {/* Error message */}
      {error && (
        <div className="text-xs text-destructive">{error}</div>
      )}

      {/* Hidden audio element for response playback */}
      {responseAudioUrl && (
        <audio
          ref={responseAudioRef}
          src={responseAudioUrl}
          className="hidden"
        />
      )}

      {/* Response display - only shown when there's a response */}
      {showResponse && lastResponse && (
        <Card className="border-primary/50">
          <CardContent className="p-3 space-y-2">
            <div className="flex items-start justify-between gap-2">
              <div className="flex-1 min-w-0">
                <p className="text-xs text-muted-foreground truncate">
                  Q: {lastResponse.question_transcript}
                </p>
                <p className="text-sm mt-1">{lastResponse.answer_text}</p>
              </div>
              <Button variant="ghost" className="h-6 w-6 p-0 shrink-0" onClick={handleSkipResponse}>
                <X className="h-3.5 w-3.5" />
              </Button>
            </div>

            {!lastResponse.has_audio && (
              <Button variant="outline" className="h-7 px-2 text-xs" onClick={handleSkipResponse}>
                {t.podcasts.resumePodcast}
              </Button>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
