import asyncio
import operator
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from content_core import extract_content
from content_core.common import ProcessSourceState
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send
from loguru import logger
from typing_extensions import Annotated, TypedDict

from open_notebook.ai.models import Model, ModelManager
from open_notebook.domain.content_settings import ContentSettings
from open_notebook.domain.notebook import Asset, Source
from open_notebook.domain.transformation import Transformation
from open_notebook.graphs.transformation import graph as transform_graph
from open_notebook.utils.video import (
    calculate_frame_params,
    cleanup_temp_files,
    extract_audio,
    extract_frames,
    get_video_duration,
)
from open_notebook.utils.video_synthesis import synthesize_video_content
from open_notebook.utils.vision import analyze_image, format_timestamp

# File extensions for visual content detection
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}


def detect_content_type(file_path: str) -> str:
    """Detect if file is image, video, or other based on extension."""
    ext = Path(file_path).suffix.lower()
    if ext in IMAGE_EXTENSIONS:
        return "image"
    elif ext in VIDEO_EXTENSIONS:
        return "video"
    return "other"


async def analyze_frames_parallel(
    frames: List[Tuple[str, float]],
    langchain_model,
    max_concurrent: int = 5,
) -> List[Tuple[float, str]]:
    """
    Analyze multiple frames in parallel with controlled concurrency.

    Args:
        frames: List of (frame_path, timestamp) tuples
        langchain_model: LangChain vision model
        max_concurrent: Maximum concurrent API calls (default 5)

    Returns:
        List of (timestamp, description) tuples for successful analyses
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def analyze_single_frame(
        frame_path: str, timestamp: float
    ) -> Tuple[float, str]:
        async with semaphore:
            ts_str = format_timestamp(timestamp)
            logger.info(f"Analyzing frame {ts_str}...")
            prompt = f"Describe what is happening in this video frame at timestamp {ts_str}. Be specific about actions, objects, and any text visible."
            try:
                desc = await analyze_image(frame_path, langchain_model, prompt)
                return (timestamp, desc)
            except Exception as e:
                logger.warning(f"Frame analysis failed at {ts_str}: {e}")
                return (timestamp, f"[Analysis failed: {e}]")

    # Create tasks for all frames
    tasks = [analyze_single_frame(path, ts) for path, ts in frames]

    # Execute in parallel with semaphore limiting concurrency
    results = await asyncio.gather(*tasks)

    logger.info(f"Completed analysis of {len(results)} frames")

    # Filter out failed analyses
    return [r for r in results if r[1] and not r[1].startswith("[Analysis failed")]


class SourceState(TypedDict):
    content_state: ProcessSourceState
    apply_transformations: List[Transformation]
    source_id: str
    notebook_ids: List[str]
    source: Source
    transformation: Annotated[list, operator.add]
    embed: bool


class TransformationState(TypedDict):
    source: Source
    transformation: Transformation


async def process_image_content(content_state: dict, file_path: str) -> dict:
    """Process image using vision model."""
    model_manager = ModelManager()

    vision_model = await model_manager.get_vision_model()
    if not vision_model:
        logger.warning("No vision model configured, skipping image analysis")
        content_state["content"] = "[Image - no vision model configured]"
        content_state["title"] = content_state.get("title") or Path(file_path).stem
        return {"content_state": ProcessSourceState(**content_state)}

    langchain_model = vision_model.to_langchain()

    prompt = """Analyze this image in detail. Describe:
1. What is shown in the image
2. Key objects, people, or elements
3. Text visible in the image (if any)
4. Overall context and meaning

Provide a comprehensive description useful for search and understanding."""

    try:
        description = await analyze_image(file_path, langchain_model, prompt)
        content_state["content"] = f"# Image Analysis\n\n{description}"
        content_state["title"] = content_state.get("title") or Path(file_path).stem
        logger.info(f"Successfully analyzed image: {file_path}")
    except Exception as e:
        logger.error(f"Image analysis failed: {e}")
        content_state["content"] = f"[Image analysis failed: {e}]"
        content_state["title"] = content_state.get("title") or Path(file_path).stem

    return {"content_state": ProcessSourceState(**content_state)}


async def process_video_content(content_state: dict, file_path: str) -> dict:
    """Process video with frame extraction + audio transcription."""
    model_manager = ModelManager()
    temp_files = []

    try:
        # 1. Get video duration and calculate optimal frame sampling parameters
        logger.info(f"Getting video duration: {file_path}")
        duration = await get_video_duration(file_path)
        fps, max_frames = calculate_frame_params(duration)
        logger.info(
            f"Video duration: {duration:.1f}s, using fps={fps}, max_frames={max_frames}"
        )

        # 2. Extract frames at calculated rate
        frames = await extract_frames(file_path, fps=fps, max_frames=max_frames)
        if frames:
            temp_files.append(str(Path(frames[0][0]).parent))

        # 3. Extract audio
        logger.info(f"Extracting audio from video: {file_path}")
        try:
            audio_path = await extract_audio(file_path)
            temp_files.append(audio_path)
        except Exception as e:
            logger.warning(f"Audio extraction failed (video may have no audio): {e}")
            audio_path = None

        # 4. Transcribe audio using existing STT
        transcript = None
        if audio_path:
            try:
                defaults = await model_manager.get_defaults()
                if defaults.default_speech_to_text_model:
                    stt_model = await Model.get(defaults.default_speech_to_text_model)
                    if stt_model:
                        audio_state = {
                            "file_path": audio_path,
                            "output_format": "markdown",
                            "audio_provider": stt_model.provider,
                            "audio_model": stt_model.name,
                        }
                        result = await extract_content(audio_state)
                        transcript = result.content
                        logger.info("Successfully transcribed video audio")
            except Exception as e:
                logger.warning(f"Audio transcription failed: {e}")

        # 5. Analyze frames with vision model (in parallel for performance)
        frame_descriptions = []
        vision_model = await model_manager.get_vision_model()

        if vision_model and frames:
            langchain_model = vision_model.to_langchain()
            logger.info(f"Analyzing {len(frames)} frames with vision model (parallel)")

            # Use parallel analysis with controlled concurrency
            frame_descriptions = await analyze_frames_parallel(
                frames, langchain_model, max_concurrent=5
            )
        elif not vision_model:
            logger.warning("No vision model configured, skipping frame analysis")

        # 6. Synthesize all content
        synthesis_model = None
        if frame_descriptions and transcript:
            chat_model = await model_manager.get_default_model("chat")
            if chat_model:
                synthesis_model = chat_model.to_langchain()

        content = await synthesize_video_content(
            frame_descriptions=frame_descriptions,
            transcript=transcript,
            model=synthesis_model,
        )

        content_state["content"] = content
        content_state["title"] = content_state.get("title") or Path(file_path).stem
        logger.info(f"Successfully processed video: {file_path}")

        return {"content_state": ProcessSourceState(**content_state)}

    except Exception as e:
        logger.error(f"Video processing failed: {e}")
        content_state["content"] = f"[Video processing failed: {e}]"
        content_state["title"] = content_state.get("title") or Path(file_path).stem
        return {"content_state": ProcessSourceState(**content_state)}

    finally:
        # Cleanup temp files
        cleanup_temp_files(temp_files)


async def content_process(state: SourceState) -> dict:
    import os
    import tempfile

    # Make a copy of content_state to avoid mutating the original (important for retries)
    content_state: Dict[str, Any] = dict(state["content_state"])  # type: ignore[assignment]

    # Store original S3 path and filename before processing (content-core needs local file)
    original_file_path = content_state.get("file_path")
    original_filename = None
    temp_file_path = None

    # If file_path is an S3 URI, download to temp file for content-core processing
    if original_file_path and original_file_path.startswith("s3://"):
        try:
            from open_notebook.utils.storage import download_file

            # Extract original filename from S3 key (e.g., s3://bucket/uploads/1/file.pdf -> file.pdf)
            original_filename = os.path.basename(original_file_path)
            file_ext = os.path.splitext(original_filename)[-1] or ".tmp"

            logger.info(f"Downloading S3 file: {original_file_path}")
            # Download S3 file to temp location
            file_content = download_file(original_file_path)
            logger.info(f"Downloaded {len(file_content)} bytes from S3")

            with tempfile.NamedTemporaryFile(
                delete=False, suffix=file_ext
            ) as tmp_file:
                tmp_file.write(file_content)
                temp_file_path = tmp_file.name

            logger.info(f"Saved S3 file to temp: {temp_file_path}")
            content_state["file_path"] = temp_file_path
        except Exception as e:
            logger.error(f"Failed to download S3 file for processing: {e}")
            raise

    # Check for visual content (images and videos)
    # Use the potentially updated file_path (temp file if S3 was downloaded)
    file_path = content_state.get("file_path")
    if file_path:
        content_type = detect_content_type(file_path)

        if content_type == "image":
            logger.info(f"Detected image file, using vision processing: {file_path}")
            result = await process_image_content(content_state, file_path)
            # Clean up temp file if we created one
            if temp_file_path:
                try:
                    os.unlink(temp_file_path)
                    logger.debug(f"Cleaned up temp file: {temp_file_path}")
                except Exception as e:
                    logger.warning(f"Failed to clean up temp file {temp_file_path}: {e}")
            # Restore original S3 path in result
            if original_file_path and original_file_path.startswith("s3://"):
                result["content_state"].file_path = original_file_path
            return result
        elif content_type == "video":
            logger.info(f"Detected video file, using vision processing: {file_path}")
            result = await process_video_content(content_state, file_path)
            # Clean up temp file if we created one
            if temp_file_path:
                try:
                    os.unlink(temp_file_path)
                    logger.debug(f"Cleaned up temp file: {temp_file_path}")
                except Exception as e:
                    logger.warning(f"Failed to clean up temp file {temp_file_path}: {e}")
            # Restore original S3 path in result
            if original_file_path and original_file_path.startswith("s3://"):
                result["content_state"].file_path = original_file_path
            return result

    # Fall through to existing content-core processing for all other content
    content_settings = ContentSettings(
        default_content_processing_engine_doc="auto",
        default_content_processing_engine_url="auto",
        default_embedding_option="ask",
        auto_delete_files="yes",
        youtube_preferred_languages=[
            "en",
            "pt",
            "es",
            "de",
            "nl",
            "en-GB",
            "fr",
            "hi",
            "ja",
        ],
    )

    content_state["url_engine"] = (
        content_settings.default_content_processing_engine_url or "auto"
    )
    content_state["document_engine"] = (
        content_settings.default_content_processing_engine_doc or "auto"
    )
    content_state["output_format"] = "markdown"

    # Add speech-to-text model configuration from Default Models
    try:
        model_manager = ModelManager()
        defaults = await model_manager.get_defaults()
        if defaults.default_speech_to_text_model:
            stt_model = await Model.get(defaults.default_speech_to_text_model)
            if stt_model:
                content_state["audio_provider"] = stt_model.provider
                content_state["audio_model"] = stt_model.name
                logger.debug(
                    f"Using speech-to-text model: {stt_model.provider}/{stt_model.name}"
                )
    except Exception as e:
        logger.warning(f"Failed to retrieve speech-to-text model configuration: {e}")
        # Continue without custom audio model (content-core will use its default)

    try:
        logger.info(f"Starting content extraction for: {content_state.get('file_path', content_state.get('url', 'unknown'))}")
        processed_state = await extract_content(content_state)
        logger.info(f"Content extraction complete, title: {getattr(processed_state, 'title', 'N/A')}")
    finally:
        # Clean up temp file if we created one
        if temp_file_path:
            try:
                os.unlink(temp_file_path)
                logger.debug(f"Cleaned up temp file: {temp_file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temp file {temp_file_path}: {e}")

    # Restore original S3 path in processed state for storage in database
    if original_file_path and original_file_path.startswith("s3://"):
        processed_state.file_path = original_file_path
        # Use original filename as title if content-core set it to temp filename
        if original_filename and (
            not processed_state.title
            or processed_state.title.startswith("tmp")
            or "tmp" in processed_state.title.lower()
        ):
            # Remove extension for cleaner title
            processed_state.title = os.path.splitext(original_filename)[0]

    return {"content_state": processed_state}


async def save_source(state: SourceState) -> dict:
    content_state = state["content_state"]

    # Get existing source using the provided source_id
    source = await Source.get(state["source_id"])
    if not source:
        raise ValueError(f"Source with ID {state['source_id']} not found")

    # Update the source with processed content
    source.asset = Asset(url=content_state.url, file_path=content_state.file_path)
    source.full_text = content_state.content

    # Preserve existing title if none provided in processed content
    if content_state.title:
        source.title = content_state.title

    await source.save()

    # NOTE: Notebook associations are created by the API immediately for UI responsiveness
    # No need to create them here to avoid duplicate edges

    if state["embed"]:
        logger.debug("Embedding content for vector search")
        await source.vectorize()

    return {"source": source}


def trigger_transformations(state: SourceState, config: RunnableConfig) -> List[Send]:
    if len(state["apply_transformations"]) == 0:
        return []

    to_apply = state["apply_transformations"]
    logger.debug(f"Applying transformations {to_apply}")

    return [
        Send(
            "transform_content",
            {
                "source": state["source"],
                "transformation": t,
            },
        )
        for t in to_apply
    ]


async def transform_content(state: TransformationState) -> Optional[dict]:
    source = state["source"]
    content = source.full_text
    if not content:
        return None
    transformation: Transformation = state["transformation"]

    logger.debug(f"Applying transformation {transformation.name}")
    result = await transform_graph.ainvoke(
        dict(input_text=content, transformation=transformation)  # type: ignore[arg-type]
    )
    await source.add_insight(transformation.title, result["output"])
    return {
        "transformation": [
            {
                "output": result["output"],
                "transformation_name": transformation.name,
            }
        ]
    }


# Create and compile the workflow
workflow = StateGraph(SourceState)

# Add nodes
workflow.add_node("content_process", content_process)
workflow.add_node("save_source", save_source)
workflow.add_node("transform_content", transform_content)
# Define the graph edges
workflow.add_edge(START, "content_process")
workflow.add_edge("content_process", "save_source")
workflow.add_conditional_edges(
    "save_source", trigger_transformations, ["transform_content"]
)
workflow.add_edge("transform_content", END)

# Compile the graph
source_graph = workflow.compile()
