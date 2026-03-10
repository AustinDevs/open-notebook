"""
Token utilities for Open Notebook.
Handles token counting and cost calculations for language models.
"""

import os

from open_notebook.config import TIKTOKEN_CACHE_DIR

# Set tiktoken cache directory before importing tiktoken to ensure
# tokenizer encodings are cached persistently in the data folder
os.environ["TIKTOKEN_CACHE_DIR"] = TIKTOKEN_CACHE_DIR

# Re-export from esperanto (source of truth for token/chunking logic)
from esperanto.utils.token_utils import (  # noqa: E402, F401
    DEFAULT_CONTEXT_LIMIT,
    DEFAULT_OUTPUT_TOKENS,
    OUTPUT_RATIO,
    SAFETY_BUFFER,
    batch_by_token_limit,
    calculate_batch_token_limit,
    calculate_output_buffer,
    chunk_text_by_tokens,
    get_context_limit_from_error,
    is_context_limit_error,
    parse_context_limit_error,
    token_count,
)


def token_cost(token_count: int, cost_per_million: float = 0.150) -> float:
    """
    Calculate the cost of tokens based on the token count and cost per million tokens.

    Args:
        token_count (int): The number of tokens.
        cost_per_million (float): The cost per million tokens. Default is 0.150.

    Returns:
        float: The calculated cost for the given token count.
    """
    return cost_per_million * (token_count / 1_000_000)
