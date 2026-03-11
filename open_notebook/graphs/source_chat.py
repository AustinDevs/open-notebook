import asyncio
import sqlite3
from typing import Annotated, Dict, Optional

from ai_prompter import Prompter
from langchain_core.messages import AIMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from loguru import logger
from typing_extensions import TypedDict

from open_notebook.ai.provision import provision_langchain_model
from open_notebook.config import LANGGRAPH_CHECKPOINT_FILE
from open_notebook.domain.notebook import (
    Source,
    format_retrieval_results,
    vector_search,
)
from open_notebook.exceptions import OpenNotebookError
from open_notebook.utils import clean_thinking_content
from open_notebook.utils.error_classifier import classify_error
from open_notebook.utils.text_utils import extract_text_content


class SourceChatState(TypedDict):
    messages: Annotated[list, add_messages]
    source_id: str
    context: Optional[str]
    model_override: Optional[str]
    context_indicators: Optional[Dict[str, list[str]]]


def call_model_with_source_context(
    state: SourceChatState, config: RunnableConfig
) -> dict:
    """
    Main function that retrieves source context via vector search and calls the model.

    This function:
    1. Fetches Source metadata (title, topics) for the system prompt
    2. Calls vector_search and post-filters by source_id (parent_id)
    3. Formats results via format_retrieval_results()
    4. Applies the source_chat Jinja2 prompt template
    5. Handles model provisioning with override support
    """
    try:
        return _call_model_with_source_context_inner(state, config)
    except OpenNotebookError:
        raise
    except Exception as e:
        error_class, user_message = classify_error(e)
        raise error_class(user_message) from e


def _call_model_with_source_context_inner(
    state: SourceChatState, config: RunnableConfig
) -> dict:
    source_id = state.get("source_id")
    if not source_id:
        raise ValueError("source_id is required in state")

    # Get the latest human message as the search query
    messages = state.get("messages", [])
    query = ""
    for msg in reversed(messages):
        if hasattr(msg, "type") and msg.type == "human":
            query = msg.content if hasattr(msg, "content") else str(msg)
            break

    # Fetch source metadata and run vector search
    def fetch_source_and_search():
        new_loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(new_loop)
            source = new_loop.run_until_complete(Source.get(source_id))
            # Request extra results to ensure enough remain after filtering
            results = []
            if query:
                results = new_loop.run_until_complete(
                    vector_search(query, 20, True, False)
                )
            return source, results
        finally:
            new_loop.close()
            asyncio.set_event_loop(None)

    try:
        asyncio.get_running_loop()
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(fetch_source_and_search)
            source, search_results = future.result()
    except RuntimeError:
        source, search_results = fetch_source_and_search()

    # Post-filter results by parent_id matching source_id
    filtered_results = [
        r for r in search_results
        if r.get("parent_id") == source_id or str(r.get("parent_id")) == source_id
    ]

    # Limit to top 10 after filtering
    filtered_results = filtered_results[:10]

    logger.info(
        f"Source chat: {len(filtered_results)} results after filtering "
        f"(from {len(search_results)} total) for source {source_id}"
    )

    # Format context
    formatted_context = format_retrieval_results(filtered_results)

    # Track context indicators
    context_indicators: Dict[str, list[str]] = {
        "sources": [],
        "insights": [],
        "notes": [],
    }
    if source and source.id:
        context_indicators["sources"].append(source.id)
    for r in filtered_results:
        rid = r.get("id", "")
        if isinstance(rid, str) and rid.startswith("source_insight:"):
            context_indicators["insights"].append(rid)

    # Build prompt data for the template
    prompt_data = {
        "source": source.model_dump() if source else None,
        "insights": [],
        "context": formatted_context,
        "context_indicators": context_indicators,
    }

    # Apply the source_chat prompt template
    system_prompt = Prompter(prompt_template="source_chat/system").render(
        data=prompt_data
    )
    payload = [SystemMessage(content=system_prompt)] + state.get("messages", [])

    # Handle async model provisioning from sync context
    def run_in_new_loop():
        """Run the async function in a new event loop"""
        new_loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(new_loop)
            return new_loop.run_until_complete(
                provision_langchain_model(
                    str(payload),
                    config.get("configurable", {}).get("model_id")
                    or state.get("model_override"),
                    "chat",
                    max_tokens=8192,
                )
            )
        finally:
            new_loop.close()
            asyncio.set_event_loop(None)

    try:
        asyncio.get_running_loop()
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(run_in_new_loop)
            model = future.result()
    except RuntimeError:
        model = asyncio.run(
            provision_langchain_model(
                str(payload),
                config.get("configurable", {}).get("model_id")
                or state.get("model_override"),
                "chat",
                max_tokens=8192,
            )
        )

    ai_message = model.invoke(payload)

    # Clean thinking content from AI response (e.g., <think>...</think> tags)
    content = extract_text_content(ai_message.content)
    cleaned_content = clean_thinking_content(content)
    cleaned_message = ai_message.model_copy(update={"content": cleaned_content})

    # Update state with context information
    # Don't persist the Source object — it's not msgpack-serializable.
    # The template already consumed it above; only serializable data goes into state.
    return {
        "messages": cleaned_message,
        "context": formatted_context,
        "context_indicators": context_indicators,
    }


# Create SQLite checkpointer
conn = sqlite3.connect(
    LANGGRAPH_CHECKPOINT_FILE,
    check_same_thread=False,
)
memory = SqliteSaver(conn)

# Create the StateGraph
source_chat_state = StateGraph(SourceChatState)
source_chat_state.add_node("source_chat_agent", call_model_with_source_context)
source_chat_state.add_edge(START, "source_chat_agent")
source_chat_state.add_edge("source_chat_agent", END)
source_chat_graph = source_chat_state.compile(checkpointer=memory)
