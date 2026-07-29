"""The socratic creator is allowlisted, loadable, and coherent with the SDK."""

from __future__ import annotations

from open_notebook_creator_sdk.schemas import SCHEMA_REGISTRY

from open_notebook.creation.registry import CREATOR_PACKAGES, _load_one


def test_socratic_is_allowlisted():
    assert CREATOR_PACKAGES["socratic"] == "socratic_creator:SocraticCreator"


def test_socratic_schema_registered_in_sdk():
    assert "socratic.v1" in SCHEMA_REGISTRY


def test_socratic_loads_with_matching_manifest():
    lc = _load_one("socratic", CREATOR_PACKAGES["socratic"])
    assert lc.available, f"socratic creator failed to load: {lc.error}"
    m = lc.manifest
    assert m.key == "socratic"
    assert m.emits == ["socratic.v1"]
