"""Assemble notebook content into a typed ContentBundle for creators.

Reuses Notebook.get_context() (the same long-form assembly the podcast path uses)
and records a token count + lightweight provenance. Optional explicit content
(already assembled by a caller) is wrapped as-is.
"""

from __future__ import annotations

from typing import List, Optional

from loguru import logger
from open_notebook_creator_sdk import ContentBundle

from open_notebook.domain.notebook import Notebook
from open_notebook.utils.token_utils import token_count


async def assemble_content(
    *,
    notebook_id: Optional[str] = None,
    content: Optional[str] = None,
) -> ContentBundle:
    """Build a ContentBundle from explicit ``content`` or a notebook's context."""
    sources_meta: List[dict] = []
    text = content

    if not text and notebook_id:
        notebook = await Notebook.get(notebook_id)
        if not notebook:
            raise ValueError(f"Notebook '{notebook_id}' not found")
        text = await notebook.get_context()
        try:
            for s in await notebook.get_sources():
                sources_meta.append({"id": str(s.id), "title": s.title or "Untitled"})
        except Exception as e:  # noqa: BLE001 - provenance is best-effort
            logger.warning(f"creation: could not collect source provenance: {e}")

    if not text:
        raise ValueError("content or notebook_id is required")

    text = str(text)
    return ContentBundle(
        text=text,
        token_count=token_count(text),
        condensed=False,
        sources=sources_meta,
    )
