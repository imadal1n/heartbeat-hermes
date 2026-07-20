from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any

from heartbeat_hermes.plugin import (
    evaluate_watch,
    heartbeat_list_tool,
    heartbeat_watch_tool,
    heartbeat_unwatch_tool,
    register,
)

def _recorder(fired: list) -> Any:
    def _wake(text: str) -> bool:
        fired.append(text)
        return True

    return _wake


PACKAGE = Path(__file__).resolve().parents[1] / "heartbeat_hermes"
ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TOOLS = ["heartbeat_watch", "heartbeat_unwatch", "heartbeat_list"]


def test_manifest_declares_exact_tools_and_hook() -> None:
    # Given: the plugin manifest.
    manifest = (PACKAGE / "plugin.yaml").read_text(encoding="utf-8")

    # When: the advertised tools and hooks are inspected.
    advertised = [
        line.strip()[2:] for line in manifest.splitlines() if line.strip().startswith("- ")
    ]

    # Then: only the expected surface is present.
    assert "name: heartbeat-hermes" in manifest
    assert advertised == ["pre_gateway_dispatch"]
    for tool in EXPECTED_TOOLS:
        assert tool in (PACKAGE / "plugin.py").read_text(encoding="utf-8")


def test_register_registers_exact_tools_and_hook() -> None:
    # Given: a Hermes-like context that records registrations.
    class Ctx:
        def __init__(self) -> None:
            self.tools: list[str] = []
            self.hooks: list[str] = []

        def register_tool(self, *, name: str, **kwargs: Any) -> None:
            self.tools.append(name)
            assert kwargs["schema"]["name"] == name
            assert callable(kwargs["handler"])

        def register_hook(self, hook_name: str, callback: Any) -> None:
            self.hooks.append(hook_name)
            assert callable(callback)

    ctx = Ctx()

    # When: the plugin registers itself.
    register(ctx)

    # Then: exactly the expected surface is registered.
    assert ctx.tools == EXPECTED_TOOLS
    assert ctx.hooks == ["pre_gateway_dispatch"]


def test_installed_directory_plugin_layout_imports(tmp_path: Path) -> None:
    # Given: package files copied into a flat Hermes directory-plugin layout.
    plugin_dir = tmp_path / "heartbeat-hermes"
    plugin_dir.mkdir()
    for name in ["__init__.py", "plugin.py", "plugin.yaml", "py.typed"]:
        _ = (plugin_dir / name).write_text((PACKAGE / name).read_text(encoding="utf-8"))
    spec = importlib.util.spec_from_file_location(
        "heartbeat_hermes_installed_test",
        plugin_dir / "__init__.py",
        submodule_search_locations=[str(plugin_dir)],
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module

    # When: the directory plugin is imported through its file location.
    spec.loader.exec_module(module)

    # Then: Hermes-visible entry points are available.
    assert isinstance(module, ModuleType)
    assert "register" in dir(module)


def test_timer_watch_fires_once_after_deadline() -> None:
    # Given: a timer watch past its deadline and a recording wake sink.
    fired: list[str] = []
    watch = {"type": "timer", "deadline": time.time() - 1, "note": "egg done"}

    # When: the watch is evaluated.
    outcome = evaluate_watch("egg", watch, time.time(), wake=_recorder(fired))

    # Then: it fires once and is removed.
    assert outcome == "remove"
    assert fired == ["[heartbeat: egg] egg done"]


def test_timer_watch_pending_before_deadline() -> None:
    # Given: a timer watch before its deadline.
    fired: list[str] = []
    watch = {"type": "timer", "deadline": time.time() + 3600}

    # When: the watch is evaluated.
    outcome = evaluate_watch("later", watch, time.time(), wake=_recorder(fired))

    # Then: nothing fires.
    assert outcome == "pending"
    assert fired == []


def test_command_watch_silent_on_empty_output() -> None:
    # Given: a command watch whose check prints nothing.
    fired: list[str] = []
    watch = {"type": "command", "command": "true"}

    # When: evaluated with a silent runner.
    outcome = evaluate_watch("quiet", watch, time.time(), wake=_recorder(fired), runner=lambda c, t: "")

    # Then: nothing fires.
    assert outcome == "pending"
    assert fired == []


def test_command_watch_fires_finding_and_repeats() -> None:
    # Given: a recurring command watch whose check prints a finding.
    fired: list[str] = []
    watch = {"type": "command", "command": "echo done"}

    # When: evaluated with a finding runner.
    outcome = evaluate_watch("job", watch, time.time(), wake=_recorder(fired), runner=lambda c, t: "done")

    # Then: the finding wakes the agent and the watch stays for the next round.
    assert outcome == "fired"
    assert fired == ["[heartbeat: job] done"]


def test_command_watch_once_removes_after_fire() -> None:
    # Given: a one-shot command watch.
    fired: list[str] = []
    watch = {"type": "command", "command": "echo done", "once": True}

    # When: it fires.
    outcome = evaluate_watch("single", watch, time.time(), wake=_recorder(fired), runner=lambda c, t: "done")

    # Then: it is removed.
    assert outcome == "remove"


def test_watch_tools_roundtrip(tmp_path: Path, monkeypatch: Any) -> None:
    # Given: an isolated state directory.
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    # When: watches are created and listed.
    heartbeat_watch_tool(name="egg", seconds=480, note="egg done")
    heartbeat_watch_tool(name="job", command="echo done", interval=30, once=True)
    import json

    listing = json.loads(heartbeat_list_tool())

    # Then: both are present with the right shapes, and removal works.
    assert listing["count"] == 2
    kinds = {w["name"]: w["type"] for w in listing["watches"]}
    assert kinds == {"egg": "timer", "job": "command"}
    heartbeat_unwatch_tool("egg")
    assert json.loads(heartbeat_list_tool())["count"] == 1


def test_public_files_do_not_leak_local_operational_data() -> None:
    # Given: files intended for a public repository.
    public_files = [
        ROOT / "README.md",
        ROOT / "pyproject.toml",
        PACKAGE / "plugin.yaml",
        PACKAGE / "plugin.py",
        PACKAGE / "__init__.py",
    ]

    # When: they are scanned for local-only operational markers.
    combined = "\n".join(path.read_text(encoding="utf-8") for path in public_files)

    # Then: only generic plugin data is present.
    assert "/home/" not in combined
    assert "Mara" not in combined
    assert "Sona" not in combined
    assert "Matrix" not in combined
    assert "openclaw" not in combined.lower()
    assert "lim.ax" not in combined
