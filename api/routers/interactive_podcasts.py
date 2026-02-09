"""
API router for interactive podcast features.

Provides endpoints for:
- Asking questions during podcast playback
- Getting contextual answers based on transcript position
"""

from fastapi import APIRouter, HTTPException
from loguru import logger
from pydantic import BaseModel, Field
from typing import Optional

from api.interactive_podcast_service import (
    InteractivePodcastService,
)

router = APIRouter()


class AskQuestionRequest(BaseModel):
    """Request model for the /ask endpoint"""

    audio_base64: str = Field(..., description="Base64 encoded audio of the question")
    current_time: float = Field(
        ..., description="Current playback position in seconds", ge=0
    )
    total_duration: float = Field(
        ..., description="Total episode duration in seconds", gt=0
    )


class AskQuestionResponse(BaseModel):
    """Response model for the /ask endpoint"""

    answer_text: str = Field(..., description="The text of the answer")
    answer_audio_base64: Optional[str] = Field(
        None, description="Base64 encoded TTS audio of the answer"
    )
    question_transcript: str = Field(..., description="Transcribed question text")
    has_audio: bool = Field(
        ..., description="Whether TTS audio is included in the response"
    )


@router.post(
    "/podcasts/episodes/{episode_id}/ask",
    response_model=AskQuestionResponse,
    summary="Ask a question during podcast playback",
    description="""
    Process a user's spoken question during interactive podcast playback.

    The endpoint:
    1. Receives audio of the user's question
    2. Transcribes it using the configured STT model
    3. Generates a contextual answer based on the podcast transcript and current position
    4. Optionally synthesizes the answer as speech using the configured TTS model

    Requires:
    - A configured STT model (for transcription)
    - A configured chat model (for answer generation)
    - Optionally, a configured TTS model (for audio response)
    """,
)
async def ask_question(episode_id: str, request: AskQuestionRequest):
    """Ask a question during podcast playback and get a contextual answer"""
    try:
        result = await InteractivePodcastService.process_question(
            episode_id=episode_id,
            audio_base64=request.audio_base64,
            current_time=request.current_time,
            total_duration=request.total_duration,
        )

        return AskQuestionResponse(
            answer_text=result.answer_text,
            answer_audio_base64=result.answer_audio_base64,
            question_transcript=result.question_transcript,
            has_audio=result.answer_audio_base64 is not None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing interactive podcast question: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process question: {str(e)}",
        )
