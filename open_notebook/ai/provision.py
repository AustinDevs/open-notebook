from esperanto import LanguageModel
from langchain_core.language_models.chat_models import BaseChatModel
from loguru import logger

from open_notebook.ai.models import model_manager
from open_notebook.utils import token_count


async def provision_langchain_model(
    content, model_id, default_type, **kwargs
) -> BaseChatModel:
    """
    Returns the best model to use based on the context size and on whether there is a specific model being requested in Config.
    If context > 105_000, returns the large_context_model
    If model_id is specified in Config, returns that model
    Otherwise, returns the default model for the given type
    """
    tokens = token_count(content)
    model = None
    selection_reason = ""

    if tokens > 105_000:
        selection_reason = f"large_context (content has {tokens} tokens)"
        logger.debug(
            f"Using large context model because the content has {tokens} tokens"
        )
        esperanto_model = await model_manager.get_default_model("large_context", **kwargs)
    elif model_id:
        selection_reason = f"explicit model_id={model_id}"
        esperanto_model = await model_manager.get_model(model_id, **kwargs)
    else:
        selection_reason = f"default for type={default_type}"
        esperanto_model = await model_manager.get_default_model(default_type, **kwargs)

    logger.debug(f"Using model: {esperanto_model}")

    if esperanto_model is None:
        logger.error(
            f"Model provisioning failed: No model found. "
            f"Selection reason: {selection_reason}. "
            f"model_id={model_id}, default_type={default_type}. "
            f"Please check Settings → Models and ensure a default model is configured for '{default_type}'."
        )
        raise ValueError(
            f"No model configured for {selection_reason}. "
            f"Please go to Settings → Models and configure a default model for '{default_type}'."
        )

    if not isinstance(esperanto_model, LanguageModel):
        logger.error(
            f"Model type mismatch: Expected LanguageModel but got {type(esperanto_model).__name__}. "
            f"Selection reason: {selection_reason}. "
            f"model_id={model_id}, default_type={default_type}."
        )
        raise ValueError(
            f"Model is not a LanguageModel: {esperanto_model}. "
            f"Please check that the model configured for '{default_type}' is a language model, not an embedding or speech model."
        )

    # Convert to LangChain model
    langchain_model = esperanto_model.to_langchain()

    # IMPORTANT: Store reference to Esperanto model on the LangChain model
    # to prevent garbage collection of the httpx clients that are shared
    # between the Esperanto model and the LangChain model.
    # Without this, the Esperanto model can be GC'd, closing the httpx clients
    # and causing "Cannot send a request, as the client has been closed" errors.
    langchain_model._esperanto_model = esperanto_model

    return langchain_model
