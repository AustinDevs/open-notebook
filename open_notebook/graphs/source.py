import operator
from typing import Any, Dict, List, Optional

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


async def content_process(state: SourceState) -> dict:
    import os
    import tempfile

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
