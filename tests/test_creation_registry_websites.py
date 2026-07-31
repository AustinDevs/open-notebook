"""The websites creator is allowlisted, loadable, and coherent with the SDK."""

from __future__ import annotations

from open_notebook_creator_sdk.schemas import SCHEMA_REGISTRY

from open_notebook.creation.registry import CREATOR_PACKAGES, _load_one


def test_websites_is_allowlisted():
    assert CREATOR_PACKAGES["websites"] == "website_creator:WebsiteCreator"


def test_website_schema_registered_in_sdk():
    assert "website.v1" in SCHEMA_REGISTRY


def test_websites_loads_with_matching_manifest():
    lc = _load_one("websites", CREATOR_PACKAGES["websites"])
    assert lc.available, f"websites creator failed to load: {lc.error}"
    m = lc.manifest
    assert m.key == "websites"
    assert m.emits == ["website.v1"]
