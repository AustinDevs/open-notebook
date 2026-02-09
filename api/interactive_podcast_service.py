"""
Service for handling interactive podcast Q&A.

This service processes user questions during podcast playback:
1. Receives audio from the user
2. Transcribes the audio using STT
3. Generates a contextual answer using LLM
4. Synthesizes the response using TTS
"""

import base64
import tempfile
from pathlib import Path
from typing import Optional, Tuple

from ai_prompter import Prompter
from fastapi import HTTPException
from loguru import logger
from pydantic import BaseModel

from open_notebook.ai.models import Model, ModelManager
from open_notebook.podcasts.models import PodcastEpisode


class InteractivePodcastRequest(BaseModel):
    """Request model for interactive podcast Q&A"""

    episode_id: str
    audio_base64: str  # Base64 encoded audio blob
    current_time: float  # Current playback position in seconds
    total_duration: float  # Total episode duration in seconds


class InteractivePodcastResponse(BaseModel):
    """Response model for interactive podcast Q&A"""

    answer_text: str  # The text of the answer
    answer_audio_base64: Optional[str] = None  # Base64 encoded TTS audio
    question_transcript: str  # What the user asked (STT result)
    has_audio: bool = False  # Whether audio is available for the response


class InteractivePodcastService:
    """Service for handling interactive podcast Q&A sessions"""

    @staticmethod
    def _extract_transcript_text(episode: PodcastEpisode) -> str:
        """Extract the full transcript text from a podcast episode"""
        if not episode.transcript:
            return ""

        transcript_entries = episode.transcript.get("transcript", [])
        if not transcript_entries:
            return ""

        lines = []
        for entry in transcript_entries:
            speaker = entry.get("speaker", "Speaker")
            dialogue = entry.get("dialogue", "")
            if dialogue:
                lines.append(f"{speaker}: {dialogue}")

        return "\n\n".join(lines)

    @staticmethod
    def _get_speaker_info(episode: PodcastEpisode) -> Tuple[bool, str]:
        """
        Extract speaker information from the episode.

        Returns:
            Tuple of (is_solo_podcast, speaker_names_str)
        """
        # Get unique speaker names from transcript
        speakers_from_transcript = set()
        if episode.transcript:
            transcript_entries = episode.transcript.get("transcript", [])
            for entry in transcript_entries:
                speaker = entry.get("speaker")
                if speaker:
                    speakers_from_transcript.add(speaker)

        # Also check speaker profile if available
        speakers_from_profile = []
        if episode.speaker_profile and isinstance(episode.speaker_profile, dict):
            profile_speakers = episode.speaker_profile.get("speakers", [])
            for speaker in profile_speakers:
                if isinstance(speaker, dict) and speaker.get("name"):
                    speakers_from_profile.append(speaker["name"])

        # Combine and deduplicate
        all_speakers = speakers_from_transcript.union(set(speakers_from_profile))

        is_solo = len(all_speakers) <= 1
        speaker_names = ", ".join(sorted(all_speakers)) if all_speakers else ""

        return is_solo, speaker_names

    @staticmethod
    def _get_context_around_position(
        episode: PodcastEpisode, current_time: float, total_duration: float
    ) -> str:
        """Get the transcript context around the current playback position"""
        if not episode.transcript:
            return ""

        transcript_entries = episode.transcript.get("transcript", [])
        if not transcript_entries:
            return ""

        # Estimate which entry we're at based on position ratio
        # This is a rough estimate since we don't have per-segment timestamps
        if total_duration <= 0:
            position_ratio = 0
        else:
            position_ratio = current_time / total_duration

        total_entries = len(transcript_entries)
        estimated_index = int(position_ratio * total_entries)

        # Get surrounding context (2 entries before and 2 after)
        start_idx = max(0, estimated_index - 2)
        end_idx = min(total_entries, estimated_index + 3)

        context_lines = []
        for i in range(start_idx, end_idx):
            entry = transcript_entries[i]
            speaker = entry.get("speaker", "Speaker")
            dialogue = entry.get("dialogue", "")
            if dialogue:
                prefix = ">>> " if i == estimated_index else ""
                context_lines.append(f"{prefix}{speaker}: {dialogue}")

        return "\n".join(context_lines)

    @staticmethod
    async def transcribe_audio(audio_base64: str) -> str:
        """Transcribe audio using the configured STT model"""
        model_manager = ModelManager()

        # Get the STT model
        stt_model = await model_manager.get_speech_to_text()
        if not stt_model:
            logger.warning("No STT model configured, returning empty transcript")
            raise HTTPException(
                status_code=400,
                detail="No speech-to-text model configured. Please configure one in Settings.",
            )

        try:
            # Decode the base64 audio
            audio_bytes = base64.b64decode(audio_base64)

            # Create a temporary file for the audio
            with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as tmp_file:
                tmp_file.write(audio_bytes)
                tmp_path = Path(tmp_file.name)

            try:
                # Transcribe using the STT model
                result = await stt_model.atranscribe(str(tmp_path))
                return result.text if hasattr(result, "text") else str(result)
            finally:
                # Clean up temp file
                tmp_path.unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"Failed to transcribe audio: {e}")
            raise HTTPException(
                status_code=500, detail=f"Failed to transcribe audio: {str(e)}"
            )

    @staticmethod
    async def generate_answer(
        question: str,
        transcript: str,
        current_time: float,
        total_duration: float,
        context_around_position: str,
        is_solo_podcast: bool = False,
        speaker_names: str = "",
    ) -> str:
        """Generate an answer using the LLM with the Q&A prompt"""
        model_manager = ModelManager()

        # Get the chat model for generating the answer
        chat_model = await model_manager.get_default_model("chat")
        if not chat_model:
            raise HTTPException(
                status_code=400,
                detail="No chat model configured. Please configure one in Settings.",
            )

        try:
            # Format the time as mm:ss
            current_time_str = f"{int(current_time // 60)}:{int(current_time % 60):02d}"
            total_duration_str = (
                f"{int(total_duration // 60)}:{int(total_duration % 60):02d}"
            )

            # Render the Q&A prompt
            prompt = Prompter(prompt_template="interactive_podcast/qa").render(
                data={
                    "transcript": transcript,
                    "current_time": current_time_str,
                    "total_duration": total_duration_str,
                    "context_around_position": context_around_position,
                    "question": question,
                    "is_solo_podcast": is_solo_podcast,
                    "speaker_names": speaker_names,
                }
            )

            # Get the LangChain-compatible model and invoke
            langchain_model = chat_model.to_langchain()
            response = await langchain_model.ainvoke(prompt)

            # Extract the content from the response
            if hasattr(response, "content"):
                return str(response.content)
            return str(response)

        except Exception as e:
            logger.error(f"Failed to generate answer: {e}")
            raise HTTPException(
                status_code=500, detail=f"Failed to generate answer: {str(e)}"
            )

    @staticmethod
    async def synthesize_speech(text: str) -> Optional[str]:
        """Synthesize speech from text using the TTS model"""
        model_manager = ModelManager()

        # Get the TTS model
        tts_model = await model_manager.get_text_to_speech()
        if not tts_model:
            logger.warning("No TTS model configured, returning text-only response")
            return None

        try:
            # Generate speech with a default voice
            # Use "alloy" as default voice (works with OpenAI-compatible providers)
            audio_result = await tts_model.agenerate_speech(text, voice="alloy")

            logger.debug(f"TTS result type: {type(audio_result)}")

            # Convert to base64
            if hasattr(audio_result, "audio_data") and audio_result.audio_data:
                audio_bytes = audio_result.audio_data
                logger.debug(f"Using audio_data, length: {len(audio_bytes)}")
            elif hasattr(audio_result, "audio") and audio_result.audio:
                audio_bytes = audio_result.audio
                logger.debug(f"Using audio, length: {len(audio_bytes)}")
            elif isinstance(audio_result, bytes):
                audio_bytes = audio_result
                logger.debug(f"Using raw bytes, length: {len(audio_bytes)}")
            else:
                logger.warning(f"Unknown audio result type: {type(audio_result)}, attrs: {dir(audio_result)}")
                return None

            return base64.b64encode(audio_bytes).decode("utf-8")

        except Exception as e:
            logger.error(f"Failed to synthesize speech: {e}")
            # Don't fail the whole request, just return None for audio
            return None

    @staticmethod
    async def process_question(
        episode_id: str,
        audio_base64: str,
        current_time: float,
        total_duration: float,
    ) -> InteractivePodcastResponse:
        """
        Process a user's question during podcast playback.

        Args:
            episode_id: The ID of the podcast episode
            audio_base64: Base64 encoded audio of the user's question
            current_time: Current playback position in seconds
            total_duration: Total episode duration in seconds

        Returns:
            InteractivePodcastResponse with the answer and optionally TTS audio
        """
        # Get the episode
        try:
            episode = await PodcastEpisode.get(episode_id)
        except Exception as e:
            logger.error(f"Failed to get episode {episode_id}: {e}")
            raise HTTPException(status_code=404, detail="Episode not found")

        # Extract the transcript
        transcript = InteractivePodcastService._extract_transcript_text(episode)
        if not transcript:
            raise HTTPException(
                status_code=400,
                detail="This episode has no transcript available for interactive Q&A",
            )

        # Get context around the current position
        context_around_position = InteractivePodcastService._get_context_around_position(
            episode, current_time, total_duration
        )

        # Get speaker information
        is_solo_podcast, speaker_names = InteractivePodcastService._get_speaker_info(
            episode
        )

        # Step 1: Transcribe the user's question
        question_transcript = await InteractivePodcastService.transcribe_audio(
            audio_base64
        )

        if not question_transcript.strip():
            raise HTTPException(
                status_code=400,
                detail="Could not understand the question. Please try again.",
            )

        logger.info(f"User question: {question_transcript}")

        # Step 2: Generate the answer
        answer_text = await InteractivePodcastService.generate_answer(
            question=question_transcript,
            transcript=transcript,
            current_time=current_time,
            total_duration=total_duration,
            context_around_position=context_around_position,
            is_solo_podcast=is_solo_podcast,
            speaker_names=speaker_names,
        )

        logger.info(f"Generated answer: {answer_text[:100]}...")

        # Step 3: Synthesize the answer as speech (optional)
        answer_audio_base64 = await InteractivePodcastService.synthesize_speech(
            answer_text
        )

        return InteractivePodcastResponse(
            answer_text=answer_text,
            answer_audio_base64=answer_audio_base64,
            question_transcript=question_transcript,
            has_audio=answer_audio_base64 is not None,
        )
