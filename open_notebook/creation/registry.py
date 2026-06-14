"""Creator discovery via an explicit allowlist + a registry digest.

Why not raw entry-point discovery? The API and the surreal-commands worker are
separate processes. If they disagree on which creators (or which versions) are
installed, the API can accept a job the worker can't run, failing mid-generation.
So:

* ``CREATOR_PACKAGES`` is the explicit, in-repo allowlist (production source of truth).
* Each process loads the allowlisted creators in isolation (one bad import can't
  take down the rest), validates ``sdk_compat``, and computes ``REGISTRY_DIGEST``.
* The API stamps the digest onto each job; the worker rejects on mismatch up front.

Installed entry points are scanned only to warn about drift (a dev aid).
"""

from __future__ import annotations

import hashlib
import importlib
import json
from importlib import metadata
from typing import Dict, Optional

from loguru import logger
from open_notebook_creator_sdk import BaseCreator, CreatorManifest, ENTRY_POINT_GROUP
from open_notebook_creator_sdk import __version__ as SDK_VERSION
from packaging.specifiers import SpecifierSet
from packaging.version import Version

# --- the allowlist: key -> "import.module:ClassName" --------------------------
CREATOR_PACKAGES: Dict[str, str] = {
    "flashcards": "flashcard_creator:FlashcardCreator",
    "charts": "chart_creator:ChartCreator",
    "infographics": "infographic_creator:InfographicCreator",
    # "podcasts": "podcast_creator.on_adapter:PodcastCreator",  # pending stateless refactor
}


class LoadedCreator:
    """A successfully loaded creator + cached manifest, or an unavailable marker."""

    def __init__(
        self,
        key: str,
        creator: Optional[BaseCreator] = None,
        manifest: Optional[CreatorManifest] = None,
        error: Optional[str] = None,
    ):
        self.key = key
        self.creator = creator
        self.manifest = manifest
        self.error = error

    @property
    def available(self) -> bool:
        return self.creator is not None and self.error is None


_REGISTRY: Dict[str, LoadedCreator] = {}
_DIGEST: str = ""


def _load_one(key: str, target: str) -> LoadedCreator:
    try:
        module_name, _, attr = target.partition(":")
        module = importlib.import_module(module_name)
        cls = getattr(module, attr)
        creator = cls()
        manifest = creator.manifest
        # validate sdk compatibility
        if Version(SDK_VERSION) not in SpecifierSet(manifest.sdk_compat):
            raise ValueError(
                f"creator '{key}' requires SDK {manifest.sdk_compat}, "
                f"host has {SDK_VERSION}"
            )
        if key != manifest.key:
            raise ValueError(
                f"allowlist key '{key}' != manifest key '{manifest.key}'"
            )
        return LoadedCreator(key, creator=creator, manifest=manifest)
    except Exception as e:  # noqa: BLE001 - one bad creator must not kill the rest
        logger.error(f"creation: failed to load creator '{key}': {e}")
        return LoadedCreator(key, error=str(e))


def _compute_digest(loaded: Dict[str, LoadedCreator]) -> str:
    payload = sorted(
        (
            {
                "key": lc.key,
                "version": lc.manifest.version if lc.manifest else None,
                "emits": lc.manifest.emits if lc.manifest else [],
                "sdk": SDK_VERSION,
            }
            for lc in loaded.values()
            if lc.available
        ),
        key=lambda d: d["key"],
    )
    blob = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def load_registry() -> None:
    """Load all allowlisted creators (idempotent). Safe to call per-process."""
    global _REGISTRY, _DIGEST
    loaded: Dict[str, LoadedCreator] = {}
    for key, target in CREATOR_PACKAGES.items():
        loaded[key] = _load_one(key, target)

    _REGISTRY = loaded
    _DIGEST = _compute_digest(loaded)

    available = [k for k, lc in loaded.items() if lc.available]
    unavailable = [k for k, lc in loaded.items() if not lc.available]
    logger.info(
        f"creation: loaded creators {available} "
        f"(unavailable: {unavailable}) digest={_DIGEST}"
    )
    _warn_entry_point_drift(loaded)


def _warn_entry_point_drift(loaded: Dict[str, LoadedCreator]) -> None:
    """Dev aid: warn about entry points installed but not allowlisted (or v.v.)."""
    try:
        eps = metadata.entry_points(group=ENTRY_POINT_GROUP)
        installed = {ep.name for ep in eps}
    except Exception:  # noqa: BLE001
        return
    allowlisted = set(CREATOR_PACKAGES)
    for name in installed - allowlisted:
        logger.warning(
            f"creation: '{name}' is installed (entry point) but not in CREATOR_PACKAGES"
        )
    for name in allowlisted - installed:
        if loaded.get(name) and loaded[name].available:
            logger.debug(
                f"creation: allowlisted '{name}' has no entry point (loaded by path)"
            )


def registry_digest() -> str:
    return _DIGEST


def all_loaded() -> Dict[str, LoadedCreator]:
    return dict(_REGISTRY)


def get_loaded(key: str) -> Optional[LoadedCreator]:
    return _REGISTRY.get(key)


def get_creator(key: str) -> BaseCreator:
    lc = _REGISTRY.get(key)
    if lc is None:
        raise ValueError(f"Unknown creator: {key}")
    if not lc.available:
        raise ValueError(f"Creator '{key}' is unavailable: {lc.error}")
    return lc.creator
