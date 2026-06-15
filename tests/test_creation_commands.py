"""Tests for the generic creation command — the count loop + anti-repeat that
generates N distinct artifacts per job (commands/creation_commands.py)."""

from unittest.mock import MagicMock, patch

import pytest

import commands.creation_commands as cc
from commands.creation_commands import CreationGenerationInput, _spec_template


def test_spec_template_extracts_antv_template():
    assert (
        _spec_template({"spec": "infographic sequence-ascending-stairs\ndata\n  x"})
        == "sequence-ascending-stairs"
    )
    assert _spec_template({"spec": "no leading infographic token"}) is None
    assert _spec_template({"foo": "bar"}) is None  # e.g. a mindmap (no spec)
    assert _spec_template(None) is None


def _fake_creator():
    manifest = MagicMock()
    manifest.model_roles = []  # no roles -> no model resolution
    manifest.emits = ["infographic.v2"]
    manifest.version = "0.3.1"
    creator = MagicMock()
    creator.manifest = manifest
    creator.config_model.model_validate = MagicMock()  # accept any config
    return creator


@pytest.mark.asyncio
async def test_count_loop_generates_n_with_antirepeat():
    creator = _fake_creator()
    calls = []

    async def fake_generate_one(**kwargs):
        calls.append(kwargs)
        art = MagicMock()
        art.id = f"creation_artifact:{len(calls)}"
        art.status = "completed"
        # distinct template each time so anti-repeat accumulates
        art.data = {"spec": f"infographic template-{len(calls)}\ndata"}
        art.error_message = None
        return art

    inp = CreationGenerationInput(
        creator_key="infographics",
        name="Doc Infographic",
        content="...",
        config={"count": 3},
        models={},
        artifact_uuid="uuid",
        instructions="focus on X",
    )
    with patch.object(cc, "get_creator", return_value=creator), patch.object(
        cc, "_generate_one", new=fake_generate_one
    ):
        out = await cc.generate_creation_artifact_command(inp)

    assert out.success is True
    assert len(calls) == 3
    assert [c["name"] for c in calls] == [
        "Doc Infographic 1",
        "Doc Infographic 2",
        "Doc Infographic 3",
    ]
    assert [c["variant_uuid"] for c in calls] == ["uuid-0", "uuid-1", "uuid-2"]
    # variety directive + user instructions on every call; anti-repeat grows
    assert "variant 1 of 3" in calls[0]["instructions"]
    assert "focus on X" in calls[0]["instructions"]
    assert "Do NOT reuse" not in calls[0]["instructions"]
    assert "template-1" in calls[1]["instructions"]
    assert "template-1" in calls[2]["instructions"]
    assert "template-2" in calls[2]["instructions"]


@pytest.mark.asyncio
async def test_count_default_one_single_artifact():
    creator = _fake_creator()
    calls = []

    async def fake_generate_one(**kwargs):
        calls.append(kwargs)
        art = MagicMock()
        art.id = "creation_artifact:1"
        art.status = "completed"
        art.data = {}
        art.error_message = None
        return art

    inp = CreationGenerationInput(
        creator_key="textbooks",
        name="My Book",
        content="...",
        config={},  # no count -> 1
        models={},
        artifact_uuid="u",
    )
    with patch.object(cc, "get_creator", return_value=creator), patch.object(
        cc, "_generate_one", new=fake_generate_one
    ):
        out = await cc.generate_creation_artifact_command(inp)

    assert out.success is True
    assert len(calls) == 1
    assert calls[0]["name"] == "My Book"  # no index suffix when single
    assert calls[0]["variant_uuid"] == "u"  # uses the base uuid
    assert calls[0]["instructions"] is None  # no variety directive for single
