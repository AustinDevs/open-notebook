'use client'

import { useState, useCallback, useRef, useEffect } from 'react'
import { useMutation } from '@tanstack/react-query'
import { podcastsApi } from '@/lib/api/podcasts'
import { InteractivePodcastResponse } from '@/lib/types/podcasts'

export type InteractiveState =
  | 'idle' // Normal playback
  | 'recording' // User is recording their question
  | 'processing' // Backend is processing the question
  | 'responding' // Playing the AI response

interface UseInteractivePodcastOptions {
  episodeId: string
  onError?: (error: Error) => void
  getCurrentTime?: () => number
  getTotalDuration?: () => number
}

interface UseInteractivePodcastReturn {
  // State
  state: InteractiveState
  isRecording: boolean
  isProcessing: boolean
  isResponding: boolean
  error: string | null

  // Audio playback
  responseAudioUrl: string | null
  lastResponse: InteractivePodcastResponse | null

  // Actions
  startRecording: () => Promise<void>
  stopRecording: () => Promise<void>
  cancelRecording: () => void
  clearResponse: () => void
}

export function useInteractivePodcast({
  episodeId,
  onError,
  getCurrentTime,
  getTotalDuration,
}: UseInteractivePodcastOptions): UseInteractivePodcastReturn {
  const [state, setState] = useState<InteractiveState>('idle')
  const [error, setError] = useState<string | null>(null)
  const [responseAudioUrl, setResponseAudioUrl] = useState<string | null>(null)
  const [lastResponse, setLastResponse] = useState<InteractivePodcastResponse | null>(
    null
  )

  // Refs for audio recording
  const mediaRecorderRef = useRef<MediaRecorder | null>(null)
  const audioChunksRef = useRef<Blob[]>([])
  const streamRef = useRef<MediaStream | null>(null)
  const currentTimeRef = useRef<number>(0)
  const totalDurationRef = useRef<number>(0)

  // Mutation for asking questions
  const askQuestionMutation = useMutation({
    mutationFn: async (audioBase64: string) => {
      // Get current playback position from getter functions or fallback to refs
      const currentTime = getCurrentTime ? getCurrentTime() : currentTimeRef.current
      const totalDuration = getTotalDuration ? getTotalDuration() : totalDurationRef.current

      return podcastsApi.askQuestion(episodeId, {
        audio_base64: audioBase64,
        current_time: currentTime,
        total_duration: totalDuration,
      })
    },
    onSuccess: (response) => {
      setLastResponse(response)

      // If there's audio, create a URL for it
      if (response.has_audio && response.answer_audio_base64) {
        // Convert base64 to blob URL
        const audioBlob = base64ToBlob(response.answer_audio_base64, 'audio/mpeg')
        const url = URL.createObjectURL(audioBlob)
        setResponseAudioUrl(url)
        setState('responding')
      } else {
        setState('idle')
      }
    },
    onError: (err) => {
      const errorMessage =
        err instanceof Error ? err.message : 'Failed to process question'
      setError(errorMessage)
      setState('idle')
      onError?.(err instanceof Error ? err : new Error(errorMessage))
    },
  })

  // Helper to convert base64 to blob
  const base64ToBlob = (base64: string, contentType: string): Blob => {
    const byteCharacters = atob(base64)
    const byteNumbers = new Array(byteCharacters.length)
    for (let i = 0; i < byteCharacters.length; i++) {
      byteNumbers[i] = byteCharacters.charCodeAt(i)
    }
    const byteArray = new Uint8Array(byteNumbers)
    return new Blob([byteArray], { type: contentType })
  }

  // Helper to convert blob to base64
  const blobToBase64 = (blob: Blob): Promise<string> => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader()
      reader.onloadend = () => {
        const result = reader.result as string
        // Remove the data URL prefix (e.g., "data:audio/webm;base64,")
        const base64 = result.split(',')[1]
        resolve(base64)
      }
      reader.onerror = reject
      reader.readAsDataURL(blob)
    })
  }

  // Start recording
  const startRecording = useCallback(async () => {
    try {
      setError(null)
      audioChunksRef.current = []

      // Get microphone access
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
      streamRef.current = stream

      // Create MediaRecorder
      const mediaRecorder = new MediaRecorder(stream, {
        mimeType: 'audio/webm;codecs=opus',
      })
      mediaRecorderRef.current = mediaRecorder

      mediaRecorder.ondataavailable = (event) => {
        if (event.data.size > 0) {
          audioChunksRef.current.push(event.data)
        }
      }

      mediaRecorder.start(100) // Collect data every 100ms
      setState('recording')
    } catch (err) {
      const errorMessage =
        err instanceof Error
          ? err.message
          : 'Failed to access microphone. Please check permissions.'
      setError(errorMessage)
      onError?.(err instanceof Error ? err : new Error(errorMessage))
    }
  }, [onError])

  // Stop recording and send to backend
  const stopRecording = useCallback(async () => {
    if (!mediaRecorderRef.current || state !== 'recording') {
      return
    }

    return new Promise<void>((resolve) => {
      const mediaRecorder = mediaRecorderRef.current!

      mediaRecorder.onstop = async () => {
        // Stop all tracks
        if (streamRef.current) {
          streamRef.current.getTracks().forEach((track) => track.stop())
          streamRef.current = null
        }

        // Combine audio chunks
        const audioBlob = new Blob(audioChunksRef.current, { type: 'audio/webm' })

        // Convert to base64 and send
        try {
          setState('processing')
          const base64Audio = await blobToBase64(audioBlob)
          await askQuestionMutation.mutateAsync(base64Audio)
        } catch {
          // Error is handled in mutation
        }

        resolve()
      }

      mediaRecorder.stop()
    })
  }, [state, askQuestionMutation])

  // Cancel recording without sending
  const cancelRecording = useCallback(() => {
    if (mediaRecorderRef.current) {
      mediaRecorderRef.current.stop()
    }

    if (streamRef.current) {
      streamRef.current.getTracks().forEach((track) => track.stop())
      streamRef.current = null
    }

    audioChunksRef.current = []
    setState('idle')
  }, [])

  // Clear the response and return to idle
  const clearResponse = useCallback(() => {
    if (responseAudioUrl) {
      URL.revokeObjectURL(responseAudioUrl)
    }
    setResponseAudioUrl(null)
    setLastResponse(null)
    setState('idle')
  }, [responseAudioUrl])

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (responseAudioUrl) {
        URL.revokeObjectURL(responseAudioUrl)
      }
      if (streamRef.current) {
        streamRef.current.getTracks().forEach((track) => track.stop())
      }
    }
  }, [responseAudioUrl])

  return {
    state,
    isRecording: state === 'recording',
    isProcessing: state === 'processing',
    isResponding: state === 'responding',
    error,
    responseAudioUrl,
    lastResponse,
    startRecording,
    stopRecording,
    cancelRecording,
    clearResponse,
  }
}
