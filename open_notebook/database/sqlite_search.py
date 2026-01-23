"""
SQLite search module for Open Notebook.
Implements vector search (using sqlite-vec) and full-text search (using FTS5).
"""

import struct
from typing import Any, Dict, List, Optional

from loguru import logger

from .sqlite_repository import db_connection, deserialize_embedding


def serialize_embedding(embedding: List[float]) -> bytes:
    """Serialize embedding list to bytes for SQLite storage."""
    return struct.pack(f"{len(embedding)}f", *embedding)


async def vector_search(
    query_embedding: List[float],
    match_count: int = 10,
    search_sources: bool = True,
    search_notes: bool = True,
    min_similarity: float = 0.2,
) -> List[Dict[str, Any]]:
    """
    Perform vector similarity search using cosine similarity.

    Since sqlite-vec may not always be available, we fall back to a pure
    Python implementation that computes cosine similarity in SQL using
    the embedding data stored as BLOBs.

    Args:
        query_embedding: The query vector
        match_count: Maximum number of results to return
        search_sources: Whether to search source embeddings and insights
        search_notes: Whether to search notes
        min_similarity: Minimum similarity threshold (0-1)

    Returns:
        List of search results with id, title, content, similarity, and parent_id
    """
    results = []

    async with db_connection() as db:
        if search_sources:
            # Search source embeddings (chunks)
            cursor = await db.execute(
                """
                SELECT
                    se.id,
                    s.id as parent_id,
                    s.title,
                    se.content,
                    se.embedding
                FROM source_embedding se
                JOIN source s ON se.source_id = s.id
                WHERE se.embedding IS NOT NULL
                """,
            )
            rows = await cursor.fetchall()

            for row in rows:
                embedding_blob = row["embedding"]
                if embedding_blob:
                    stored_embedding = deserialize_embedding(embedding_blob)
                    if stored_embedding and len(stored_embedding) == len(query_embedding):
                        similarity = _cosine_similarity(query_embedding, stored_embedding)
                        if similarity >= min_similarity:
                            results.append({
                                "id": f"source_embedding:{row['id']}",
                                "parent_id": f"source:{row['parent_id']}",
                                "title": row["title"] or "",
                                "content": row["content"],
                                "similarity": similarity,
                                "type": "source_embedding",
                            })

            # Search source insights
            cursor = await db.execute(
                """
                SELECT
                    si.id,
                    s.id as parent_id,
                    si.insight_type,
                    s.title as source_title,
                    si.content,
                    si.embedding
                FROM source_insight si
                JOIN source s ON si.source_id = s.id
                WHERE si.embedding IS NOT NULL
                """,
            )
            rows = await cursor.fetchall()

            for row in rows:
                embedding_blob = row["embedding"]
                if embedding_blob:
                    stored_embedding = deserialize_embedding(embedding_blob)
                    if stored_embedding and len(stored_embedding) == len(query_embedding):
                        similarity = _cosine_similarity(query_embedding, stored_embedding)
                        if similarity >= min_similarity:
                            title = f"{row['insight_type']} - {row['source_title'] or ''}"
                            results.append({
                                "id": f"source_insight:{row['id']}",
                                "parent_id": f"source:{row['parent_id']}",
                                "title": title,
                                "content": row["content"],
                                "similarity": similarity,
                                "type": "source_insight",
                            })

        if search_notes:
            # Search notes
            cursor = await db.execute(
                """
                SELECT
                    id,
                    title,
                    content,
                    embedding
                FROM note
                WHERE embedding IS NOT NULL
                """,
            )
            rows = await cursor.fetchall()

            for row in rows:
                embedding_blob = row["embedding"]
                if embedding_blob:
                    stored_embedding = deserialize_embedding(embedding_blob)
                    if stored_embedding and len(stored_embedding) == len(query_embedding):
                        similarity = _cosine_similarity(query_embedding, stored_embedding)
                        if similarity >= min_similarity:
                            results.append({
                                "id": f"note:{row['id']}",
                                "parent_id": f"note:{row['id']}",
                                "title": row["title"] or "",
                                "content": row["content"],
                                "similarity": similarity,
                                "type": "note",
                            })

    # Sort by similarity (descending) and limit
    results.sort(key=lambda x: x["similarity"], reverse=True)

    # Group by parent_id and take max similarity (similar to SurrealDB behavior)
    seen_parents = {}
    deduplicated = []
    for result in results:
        parent_id = result["parent_id"]
        if parent_id not in seen_parents:
            seen_parents[parent_id] = result
            deduplicated.append(result)
        elif result["similarity"] > seen_parents[parent_id]["similarity"]:
            # Replace with higher similarity result
            idx = deduplicated.index(seen_parents[parent_id])
            deduplicated[idx] = result
            seen_parents[parent_id] = result

    # Re-sort and limit
    deduplicated.sort(key=lambda x: x["similarity"], reverse=True)
    return deduplicated[:match_count]


async def text_search(
    query_text: str,
    match_count: int = 10,
    search_sources: bool = True,
    search_notes: bool = True,
) -> List[Dict[str, Any]]:
    """
    Perform full-text search using FTS5.

    Args:
        query_text: The search query
        match_count: Maximum number of results to return
        search_sources: Whether to search sources
        search_notes: Whether to search notes

    Returns:
        List of search results with id, title, snippet, and relevance score
    """
    results = []

    # Escape special FTS5 characters and prepare query
    # FTS5 uses double quotes for phrases
    safe_query = query_text.replace('"', '""')

    async with db_connection() as db:
        if search_sources:
            # Search source titles and full_text
            try:
                cursor = await db.execute(
                    """
                    SELECT
                        s.id,
                        s.title,
                        snippet(source_fts, 1, '<mark>', '</mark>', '...', 64) as snippet,
                        bm25(source_fts) as rank
                    FROM source_fts
                    JOIN source s ON source_fts.rowid = s.id
                    WHERE source_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                    """,
                    (safe_query, match_count),
                )
                rows = await cursor.fetchall()

                for row in rows:
                    results.append({
                        "item_id": f"source:{row['id']}",
                        "title": row["title"] or "",
                        "snippet": row["snippet"],
                        "relevance": -row["rank"],  # BM25 returns negative scores
                        "type": "source",
                    })
            except Exception as e:
                logger.debug(f"Source FTS search failed: {e}")

            # Search source embeddings (chunks)
            try:
                cursor = await db.execute(
                    """
                    SELECT
                        se.source_id,
                        s.title,
                        snippet(source_embedding_fts, 0, '<mark>', '</mark>', '...', 64) as snippet,
                        bm25(source_embedding_fts) as rank
                    FROM source_embedding_fts
                    JOIN source_embedding se ON source_embedding_fts.rowid = se.id
                    JOIN source s ON se.source_id = s.id
                    WHERE source_embedding_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                    """,
                    (safe_query, match_count),
                )
                rows = await cursor.fetchall()

                for row in rows:
                    results.append({
                        "item_id": f"source:{row['source_id']}",
                        "title": row["title"] or "",
                        "snippet": row["snippet"],
                        "relevance": -row["rank"],
                        "type": "source_chunk",
                    })
            except Exception as e:
                logger.debug(f"Source embedding FTS search failed: {e}")

            # Search source insights
            try:
                cursor = await db.execute(
                    """
                    SELECT
                        si.source_id,
                        si.insight_type,
                        s.title as source_title,
                        snippet(source_insight_fts, 0, '<mark>', '</mark>', '...', 64) as snippet,
                        bm25(source_insight_fts) as rank
                    FROM source_insight_fts
                    JOIN source_insight si ON source_insight_fts.rowid = si.id
                    JOIN source s ON si.source_id = s.id
                    WHERE source_insight_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                    """,
                    (safe_query, match_count),
                )
                rows = await cursor.fetchall()

                for row in rows:
                    title = f"{row['insight_type']} - {row['source_title'] or ''}"
                    results.append({
                        "item_id": f"source:{row['source_id']}",
                        "title": title,
                        "snippet": row["snippet"],
                        "relevance": -row["rank"],
                        "type": "source_insight",
                    })
            except Exception as e:
                logger.debug(f"Source insight FTS search failed: {e}")

        if search_notes:
            # Search notes
            try:
                cursor = await db.execute(
                    """
                    SELECT
                        n.id,
                        n.title,
                        snippet(note_fts, 1, '<mark>', '</mark>', '...', 64) as snippet,
                        bm25(note_fts) as rank
                    FROM note_fts
                    JOIN note n ON note_fts.rowid = n.id
                    WHERE note_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                    """,
                    (safe_query, match_count),
                )
                rows = await cursor.fetchall()

                for row in rows:
                    results.append({
                        "item_id": f"note:{row['id']}",
                        "title": row["title"] or "",
                        "snippet": row["snippet"],
                        "relevance": -row["rank"],
                        "type": "note",
                    })
            except Exception as e:
                logger.debug(f"Note FTS search failed: {e}")

    # Sort by relevance (descending) and deduplicate by item_id
    results.sort(key=lambda x: x["relevance"], reverse=True)

    # Deduplicate by item_id, keeping highest relevance
    seen = {}
    deduplicated = []
    for result in results:
        item_id = result["item_id"]
        if item_id not in seen:
            seen[item_id] = result
            deduplicated.append(result)
        elif result["relevance"] > seen[item_id]["relevance"]:
            idx = deduplicated.index(seen[item_id])
            deduplicated[idx] = result
            seen[item_id] = result

    return deduplicated[:match_count]


def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """
    Compute cosine similarity between two vectors.

    Returns a value between -1 and 1, where 1 means identical direction.
    """
    if len(vec1) != len(vec2):
        return 0.0

    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = sum(a * a for a in vec1) ** 0.5
    norm2 = sum(b * b for b in vec2) ** 0.5

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return dot_product / (norm1 * norm2)
