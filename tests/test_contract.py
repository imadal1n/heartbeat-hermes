from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

import heartbeat_hermes.plugin as plugin
from heartbeat_hermes.plugin import (
    evaluate_watch,
    heartbeat_list_tool,
    heartbeat_watch_tool,
    heartbeat_unwatch_tool,
    register,
)


@pytest.fixture(autouse=True)
def _reset_plugin_state(monkeypatch: Any) -> Any:
    snapshot = {
        name: getattr(plugin, name)
        for name in (
            "_gateway_runner",
            "_gateway_loop",
            "_routing",
            "_pinned_routing",
            "_owns_scheduler_lock",
            "_scheduler_thread",
        )
    }
    plugin._gateway_runner = None
    plugin._gateway_loop = None
    plugin._routing = None
    plugin._pinned_routing = None
    plugin._owns_scheduler_lock = False
    plugin._scheduler_thread = None
    monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", lambda: False)
    yield
    for name, value in snapshot.items():
        setattr(plugin, name, value)


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
    outcome = evaluate_watch("later", watch, time.time(), wake=fired.append)

    # Then: nothing fires.
    assert outcome == "pending"
    assert fired == []


def test_repeating_timer_rearms_after_fire() -> None:
    # Given: a repeating timer past its deadline.
    fired: list[str] = []
    now = time.time()
    watch = {"type": "timer", "deadline": now - 1, "seconds": 480, "repeat": True, "note": "egg done"}

    # When: it fires.
    outcome = evaluate_watch("egg", watch, now, wake=_recorder(fired))

    # Then: it stays armed with a fresh deadline instead of being removed.
    assert outcome == "fired"
    assert fired == ["[heartbeat: egg] egg done"]
    assert watch["deadline"] > now + 400


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


def test_scheduler_not_armed_without_lock() -> None:
    # Given: a plugin registered while another process owns the scheduler lock.
    class Ctx:
        def register_tool(self, **kwargs: Any) -> None:
            pass

        def register_hook(self, hook_name: str, callback: Any) -> None:
            pass

    register(Ctx())

    # When: the gateway-capture hook fires on an incoming message.
    plugin._capture_gateway(gateway=object(), event=None)

    # Then: no scheduler thread starts in this process.
    assert plugin._owns_scheduler_lock is False
    assert plugin._scheduler_thread is None


def test_gateway_capture_retries_scheduler_lock_after_register_miss(monkeypatch: Any) -> None:
    # Given: registration happens before this process can own the scheduler lock.
    lock_results = iter([False, True])
    ensure_calls = 0

    def _try_lock() -> bool:
        return next(lock_results)

    def _record_ensure() -> None:
        nonlocal ensure_calls
        ensure_calls += 1

    monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", _try_lock)
    monkeypatch.setattr(plugin, "_ensure_scheduler", _record_ensure)

    class Ctx:
        def register_tool(self, **kwargs: Any) -> None:
            pass

        def register_hook(self, hook_name: str, callback: Any) -> None:
            pass

    register(Ctx())
    assert plugin._owns_scheduler_lock is False

    # When: the gateway is later captured and the lock is now available.
    plugin._capture_gateway(gateway=object(), event=None)

    # Then: this process becomes the scheduler owner and arms it.
    assert plugin._owns_scheduler_lock is True
    assert ensure_calls == 1


def test_command_watch_rejects_mcp_tool_name_as_shell_command(tmp_path: Path, monkeypatch: Any) -> None:
    # Given: an isolated state directory.
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    # When: a command watch is created with an MCP tool name instead of a shell command.
    import json

    result = json.loads(
        heartbeat_watch_tool(
            name="bad",
            command="mcp__opencode__opencode_check session-id",
            interval=30,
        )
    )

    # Then: the invalid watch is rejected before it can stall the scheduler.
    assert result == {
        "error": "command must be a shell command, not an MCP tool name; use a real executable or script"
    }
    assert json.loads(heartbeat_list_tool()) == {"watches": [], "count": 0}


def test_routing_is_sticky_first_capture() -> None:
    # Given: two incoming messages from different conversations.
    first = SimpleNamespace(
        source=SimpleNamespace(
            platform="telegram", chat_id="room-a", chat_name=None, chat_type="group", thread_id=None
        )
    )
    second = SimpleNamespace(
        source=SimpleNamespace(
            platform="telegram", chat_id="room-b", chat_name=None, chat_type="group", thread_id=None
        )
    )

    # When: the capture hook sees both.
    plugin._capture_gateway(gateway=None, event=first)
    plugin._capture_gateway(gateway=None, event=second)

    # Then: the wake target stays the first conversation, not the last.
    assert plugin._routing is not None
    assert plugin._routing["chat_id"] == "room-a"


def test_pinned_routing_from_env_overrides_capture(monkeypatch: Any) -> None:
    # Given: an explicit wake target pinned via env and a later message from elsewhere.
    monkeypatch.setenv("HEARTBEAT_DELIVER_PLATFORM", "telegram")
    monkeypatch.setenv("HEARTBEAT_DELIVER_CHAT_ID", "pinned-room")

    class Ctx:
        def register_tool(self, **kwargs: Any) -> None:
            pass

        def register_hook(self, hook_name: str, callback: Any) -> None:
            pass

    register(Ctx())
    event = SimpleNamespace(
        source=SimpleNamespace(
            platform="telegram", chat_id="other-room", chat_name=None, chat_type="dm", thread_id=None
        )
    )

    # When: the capture hook sees the message.
    plugin._capture_gateway(gateway=None, event=event)

    # Then: the pinned target wins.
    assert plugin._routing is not None
    assert plugin._routing["chat_id"] == "pinned-room"


def test_concurrent_watch_writes_do_not_crash(tmp_path: Path, monkeypatch: Any) -> None:
    # Given: many threads writing watches concurrently against one state file.
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    import threading

    errors: list[Exception] = []

    def _writer(i: int) -> None:
        try:
            heartbeat_watch_tool(name=f"w{i}", command=f"echo {i}", interval=30)
        except Exception as exc:  # noqa: BLE001 - test records any failure
            errors.append(exc)

    threads = [threading.Thread(target=_writer, args=(i,)) for i in range(20)]

    # When: they all write at once.
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Then: no write crashes and every watch survives.
    import json

    assert errors == []
    assert json.loads(heartbeat_list_tool())["count"] == 20


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
