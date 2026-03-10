"""
Default model seeder: automatically assigns synced models as defaults
and populates podcast profile model references on API startup.

Runs after migrations and podcast profile migration. Idempotent and
non-destructive — never overwrites existing user choices.

If NOTEBOOKER_OPENAI_COMPATIBLE_* env vars are set, automatically
creates a credential and registers seed models before proceeding.
"""

import os
from typing import Optional

import httpx
from loguru import logger
from pydantic import SecretStr

from open_notebook.database.repository import ensure_record_id, repo_query, repo_update

# Well-known model names from the Cloudflare unified worker
SEED_MODELS = {
    "language": "mistralai/mistral-small-3.1-24b-instruct",
    "embedding": "google/embeddinggemma-300m",
    "speech_to_text": "whisper-large-v3-turbo",
    "text_to_speech": "tts-aura-1",
}

# Inverted SEED_MODELS: model_name → model_type (for classifying discovered models)
SEED_MODEL_TYPES = {name: model_type for model_type, name in SEED_MODELS.items()}

# Map speaker names (lowercased) to Aura-1 voice IDs
SPEAKER_VOICE_MAP = {
    "marcus thompson": "arcas",
    "elena vasquez": "athena",
    "johny bing": "orion",
    "professor sarah kim": "asteria",
    "dr. alex chen": "orpheus",
    "jamie rodriguez": "zeus",
}

# Env vars for auto-provisioning
NOTEBOOKER_ENV_VARS = {
    "api_key": "NOTEBOOKER_OPENAI_COMPATIBLE_API_KEY",
    "base_url": "NOTEBOOKER_OPENAI_COMPATIBLE_BASE_URL_LLM",
}


# =============================================================================
# Auto-provisioning: Credential + Model registration from env vars
# =============================================================================


def _get_notebooker_config() -> Optional[dict]:
    """Read NOTEBOOKER_ env vars for openai_compatible auto-provisioning.

    Returns dict with 'base_url' and 'api_key' if base_url is set,
    otherwise None.
    """
    base_url = os.environ.get(NOTEBOOKER_ENV_VARS["base_url"], "").strip()
    api_key = os.environ.get(NOTEBOOKER_ENV_VARS["api_key"], "").strip()

    if not base_url:
        return None

    return {"base_url": base_url, "api_key": api_key}


async def _ensure_credential(config: dict) -> Optional[str]:
    """Create an openai_compatible Credential if none exists.

    Returns the credential ID if created or already exists, None on failure.
    Idempotent: skips creation if any openai_compatible credential exists.
    """
    from open_notebook.domain.credential import Credential

    try:
        existing = await Credential.get_by_provider("openai_compatible")
        if existing:
            logger.info(
                f"openai_compatible credential already exists "
                f"({len(existing)} found), skipping creation"
            )
            return str(existing[0].id)
    except Exception as e:
        logger.warning(f"Failed to check existing credentials: {e}")
        return None

    try:
        api_key = config.get("api_key")
        base_url = config.get("base_url")

        cred = Credential(
            name="Notebooker (auto-provisioned)",
            provider="openai_compatible",
            modalities=["language", "embedding", "speech_to_text", "text_to_speech"],
            api_key=SecretStr(api_key) if api_key else None,
            base_url=base_url,
        )
        await cred.save()
        logger.info(f"Created openai_compatible credential: {cred.id}")
        return str(cred.id)
    except Exception as e:
        logger.warning(f"Failed to create openai_compatible credential: {e}")
        return None


async def _discover_models(config: dict) -> list[dict]:
    """Discover models from an OpenAI-compatible /models endpoint.

    Returns list of dicts with 'id' keys, or empty list on failure.
    """
    base_url = config["base_url"].rstrip("/")
    api_key = config.get("api_key")

    try:
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{base_url}/models",
                headers=headers,
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()

            models = [
                {"id": m.get("id", "")}
                for m in data.get("data", [])
                if m.get("id")
            ]

            logger.info(f"Discovered {len(models)} model(s) from {base_url}/models")
            return models

    except httpx.HTTPStatusError as e:
        logger.warning(
            f"Model discovery HTTP error: {e.response.status_code} "
            f"from {base_url}/models"
        )
    except Exception as e:
        logger.warning(f"Model discovery failed from {base_url}/models: {e}")

    return []


async def _register_models(
    discovered: list[dict], credential_id: str
) -> dict[str, str]:
    """Register discovered models, classifying by SEED_MODELS names.

    Only registers models whose names appear in SEED_MODEL_TYPES.
    Returns dict of model_type → model_id for successfully registered models.
    """
    from open_notebook.ai.models import Model

    # Batch-fetch existing models to avoid duplicates
    try:
        existing = await repo_query(
            "SELECT string::lowercase(name) as name, string::lowercase(type) as type "
            "FROM model WHERE string::lowercase(provider) = 'openai_compatible'",
            {},
        )
        existing_keys = {(m["name"], m["type"]) for m in existing}
    except Exception:
        existing_keys = set()

    registered: dict[str, str] = {}

    for model_data in discovered:
        model_name = model_data["id"]

        # Only register models that match our seed model names
        model_type = SEED_MODEL_TYPES.get(model_name)
        if not model_type:
            logger.debug(f"Skipping non-seed model: {model_name}")
            continue

        # Check for duplicates (case-insensitive)
        key = (model_name.lower(), model_type.lower())
        if key in existing_keys:
            logger.debug(f"Model already registered: {model_name} ({model_type})")
            existing_model_id = await _find_model_by_name(model_name, model_type)
            if existing_model_id:
                registered[model_type] = existing_model_id
            continue

        try:
            new_model = Model(
                name=model_name,
                provider="openai_compatible",
                type=model_type,
                credential=credential_id,
            )
            await new_model.save()
            logger.info(
                f"Registered model: {model_name} as {model_type} "
                f"(credential={credential_id})"
            )
            if new_model.id:
                registered[model_type] = str(new_model.id)
        except Exception as e:
            logger.warning(f"Failed to register model {model_name}: {e}")

    return registered


async def _auto_provision_models() -> None:
    """Auto-provision credential and models from NOTEBOOKER_ env vars.

    Reads NOTEBOOKER_OPENAI_COMPATIBLE_* env vars, creates a Credential
    if needed, discovers models from the endpoint, and registers seed
    models in the database. Idempotent and non-destructive.
    """
    config = _get_notebooker_config()
    if not config:
        logger.debug("No NOTEBOOKER_ env vars found, skipping auto-provisioning")
        return

    logger.info("NOTEBOOKER_ env vars detected, starting auto-provisioning...")

    # Step 1: Ensure credential exists
    credential_id = await _ensure_credential(config)
    if not credential_id:
        logger.warning("Could not create/find credential, skipping model registration")
        return

    # Step 2: Discover models from endpoint
    discovered = await _discover_models(config)
    if not discovered:
        logger.warning(
            "No models discovered from endpoint. "
            "Seed models may need to be registered manually."
        )
        return

    # Step 3: Register seed models
    registered = await _register_models(discovered, credential_id)

    if registered:
        logger.info(
            f"Auto-provisioned {len(registered)} model(s): "
            f"{', '.join(f'{t}={n}' for t, n in registered.items())}"
        )
    else:
        logger.info(
            "No new seed models registered "
            "(may already exist or not found in endpoint)"
        )


# =============================================================================
# Model lookup + default seeding
# =============================================================================


async def _find_model_by_name(name: str, model_type: str) -> str | None:
    """Find an existing Model record matching name + type."""
    results = await repo_query(
        "SELECT * FROM model WHERE name = $name AND type = $type LIMIT 1",
        {"name": name, "type": model_type},
    )
    if results:
        return str(results[0]["id"])
    return None


async def _seed_default_models(model_ids: dict[str, str]) -> None:
    """Set empty default model slots from resolved model IDs."""
    from open_notebook.ai.models import DefaultModels

    defaults = await DefaultModels.get_instance()

    language_id = model_ids.get("language")
    embedding_id = model_ids.get("embedding")
    stt_id = model_ids.get("speech_to_text")
    tts_id = model_ids.get("text_to_speech")

    slot_map = {
        "default_chat_model": language_id,
        "default_transformation_model": language_id,
        "large_context_model": language_id,
        "default_vision_model": language_id,
        "default_tools_model": language_id,
        "default_embedding_model": embedding_id,
        "default_speech_to_text_model": stt_id,
        "default_text_to_speech_model": tts_id,
    }

    changed = False
    for slot, candidate_id in slot_map.items():
        current = getattr(defaults, slot, None)
        if not current and candidate_id:
            logger.info(f"Setting default {slot} → {candidate_id}")
            setattr(defaults, slot, candidate_id)
            changed = True
        elif current:
            logger.debug(f"Default {slot} already configured: {current}")

    if changed:
        await defaults.update()
        logger.info("Default model assignments updated")
    else:
        logger.info("All default model slots already configured")


async def _seed_episode_profiles(language_model_id: str) -> None:
    """Set outline_llm and transcript_llm on episode profiles if empty."""
    profiles = await repo_query("SELECT * FROM episode_profile")
    seeded = 0

    for raw in profiles:
        profile_name = raw.get("name", raw.get("id", "unknown"))
        updates = {}

        if not raw.get("outline_llm"):
            updates["outline_llm"] = ensure_record_id(language_model_id)
        if not raw.get("transcript_llm"):
            updates["transcript_llm"] = ensure_record_id(language_model_id)

        if updates:
            await repo_update("episode_profile", str(raw["id"]), updates)
            seeded += 1
            logger.info(
                f"Seeded episode profile '{profile_name}': {list(updates.keys())}"
            )

    if seeded:
        logger.info(f"Seeded {seeded} episode profile(s)")
    else:
        logger.info("All episode profiles already have LLM assignments")


async def _seed_speaker_profiles(tts_model_id: str) -> None:
    """Set voice_model on speaker profiles if empty."""
    profiles = await repo_query("SELECT * FROM speaker_profile")
    seeded = 0

    for raw in profiles:
        profile_name = raw.get("name", raw.get("id", "unknown"))

        if not raw.get("voice_model"):
            await repo_update(
                "speaker_profile",
                str(raw["id"]),
                {"voice_model": ensure_record_id(tts_model_id)},
            )
            seeded += 1
            logger.info(f"Seeded speaker profile '{profile_name}' with voice_model")

    if seeded:
        logger.info(f"Seeded {seeded} speaker profile(s)")
    else:
        logger.info("All speaker profiles already have voice_model assignments")


async def _remap_speaker_voice_ids() -> None:
    """Remap voice_id fields in speaker profiles to match Aura-1 voices."""
    profiles = await repo_query("SELECT * FROM speaker_profile")
    remapped = 0

    for raw in profiles:
        profile_name = raw.get("name", raw.get("id", "unknown"))
        speakers = raw.get("speakers", [])
        if not speakers:
            continue

        updated_speakers = []
        profile_changed = False

        for speaker in speakers:
            name_lower = speaker.get("name", "").lower()
            if name_lower in SPEAKER_VOICE_MAP:
                new_voice_id = SPEAKER_VOICE_MAP[name_lower]
                if speaker.get("voice_id") != new_voice_id:
                    speaker["voice_id"] = new_voice_id
                    profile_changed = True
            updated_speakers.append(speaker)

        if profile_changed:
            await repo_update(
                "speaker_profile",
                str(raw["id"]),
                {"speakers": updated_speakers},
            )
            remapped += 1
            logger.info(f"Remapped voice IDs in speaker profile '{profile_name}'")

    if remapped:
        logger.info(f"Remapped voice IDs in {remapped} speaker profile(s)")
    else:
        logger.info("All speaker voice IDs already correct")


# =============================================================================
# Main entry point
# =============================================================================


async def seed_default_models() -> None:
    """Main entry point: resolve synced models and seed defaults.

    Idempotent and non-destructive. Runs on every startup but only
    modifies fields that are currently unset.

    If NOTEBOOKER_OPENAI_COMPATIBLE_* env vars are set, automatically
    creates a credential and registers seed models before proceeding.
    """
    logger.info("Starting default model seeding...")

    # Auto-provision from NOTEBOOKER_ env vars (before resolving models)
    await _auto_provision_models()

    # Resolve model IDs by name + type
    model_ids: dict[str, str] = {}
    for model_type, model_name in SEED_MODELS.items():
        model_id = await _find_model_by_name(model_name, model_type)
        if model_id:
            model_ids[model_type] = model_id
            logger.debug(f"Found {model_type} model: {model_name} → {model_id}")
        else:
            logger.warning(
                f"Seed model not found: {model_name} (type={model_type}). "
                "Has credential discovery/sync been run?"
            )

    if not model_ids:
        logger.info(
            "No seed models found in database. "
            "Skipping default seeding (models may not be synced yet)."
        )
        return

    # Seed DefaultModels singleton
    await _seed_default_models(model_ids)

    # Seed episode profiles (needs language model)
    language_id = model_ids.get("language")
    if language_id:
        await _seed_episode_profiles(language_id)

    # Seed speaker profiles (needs TTS model)
    tts_id = model_ids.get("text_to_speech")
    if tts_id:
        await _seed_speaker_profiles(tts_id)

    # Remap speaker voice IDs (always runs, independent of model lookup)
    await _remap_speaker_voice_ids()

    logger.info("Default model seeding complete")


if __name__ == "__main__":
    import asyncio

    from dotenv import load_dotenv

    load_dotenv()
    asyncio.run(seed_default_models())
