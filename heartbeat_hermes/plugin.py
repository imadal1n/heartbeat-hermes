"""Generic wake-on-done heartbeat for Hermes.

Watches anything that can produce a "done": timers, check commands, process
exits, external session state. When a watch fires, the agent is woken in its
main session through a synthetic internal MessageEvent. The plugin never
posts to any chat itself; the agent investigates and answers itself.

Watch types:

- ``timer``: one-shot. Fires once ``seconds`` have elapsed since creation.
- ``command``: recurring. Runs a shell check command every ``interval``
  seconds; empty stdout means "nothing", non-empty stdout is the finding
  that wakes the agent. With ``once: true`` the watch is removed after the
  first fire.

The wake path reuses the gateway's own synthetic-event pipeline: a
``MessageEvent(internal=True)`` is handed to the captured platform adapter,
which routes it through normal dispatch into the agent's active session.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

POLL_SECONDS = 5.0
COMMAND_TIMEOUT_SECONDS = 120.0
TIMER_MIN_INTERVAL = 5
STATE_DIRNAME = "heartbeat"
WATCHES_FILENAME = "watches.json"
LOCK_FILENAME = "scheduler.lock"

_gateway_runner: Any = None
_gateway_loop: Any = None
_routing: Optional[Dict[str, Any]] = None
_pinned_routing: Optional[Dict[str, Any]] = None
_owns_scheduler_lock = False
_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop = threading.Event()
_scheduler_lock_fd: Any = None
_state_lock = threading.Lock()


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def _state_dir() -> Path:
    home = os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes")
    path = Path(home) / STATE_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_watches() -> Dict[str, Dict[str, Any]]:
    try:
        data = json.loads((_state_dir() / WATCHES_FILENAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_watches_locked(watches: Dict[str, Dict[str, Any]]) -> None:
    """Write watches atomically. Caller must hold ``_state_lock``."""
    target = _state_dir() / WATCHES_FILENAME
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(watches, indent=2), encoding="utf-8")
    tmp.replace(target)


# ---------------------------------------------------------------------------
# Wake injection
# ---------------------------------------------------------------------------


def _resolve_adapter(runner: Any, platform_value: str) -> Any:
    for platform, adapter in getattr(runner, "adapters", {}).items():
        value = platform.value if hasattr(platform, "value") else str(platform)
        if value == platform_value:
            return adapter
    return None


def _inject_wake(text: str) -> bool:
    """Hand a synthetic internal event to the captured gateway adapter."""
    global _gateway_runner, _gateway_loop
    if not _gateway_runner or not _gateway_loop or not _routing:
        logger.warning("heartbeat: gateway/routing not captured yet, wake dropped: %s", text[:80])
        return False
    try:
        from gateway.config import Platform
        from gateway.platforms.base import MessageEvent, MessageType
        from gateway.session import SessionSource

        platform_value = str(_routing.get("platform", ""))
        try:
            platform = Platform(platform_value)
        except ValueError:
            logger.error("heartbeat: unknown platform %r", platform_value)
            return False

        source = SessionSource(
            platform=platform,
            chat_id=str(_routing.get("chat_id", "")),
            chat_name=_routing.get("chat_name"),
            chat_type=_routing.get("chat_type", "dm"),
            user_id="system:heartbeat",
            user_name="heartbeat",
            thread_id=_routing.get("thread_id"),
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            internal=True,
        )
        adapter = _resolve_adapter(_gateway_runner, platform_value)
        if adapter is None:
            logger.error("heartbeat: no adapter for platform %r", platform_value)
            return False

        async def _deliver() -> None:
            await adapter.handle_message(event)

        future = asyncio.run_coroutine_threadsafe(_deliver(), _gateway_loop)
        future.result(timeout=15)
        logger.info("heartbeat: wake delivered: %s", text[:80])
        return True
    except Exception as exc:  # noqa: BLE001 - wake must never kill the scheduler
        logger.error("heartbeat: wake injection failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Watch evaluation
# ---------------------------------------------------------------------------


def _run_check(command: str, timeout: float) -> str:
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        logger.warning("heartbeat: check command failed: %s", exc)
        return ""
    return (result.stdout or "").strip()


def evaluate_watch(
    name: str,
    watch: Dict[str, Any],
    now: float,
    wake: Callable[[str], bool] = _inject_wake,
    runner: Callable[[str, float], str] = _run_check,
) -> str:
    """Evaluate one due watch. Returns "remove", "fired" or "pending"."""
    kind = watch.get("type", "command")
    if kind == "timer":
        if now < float(watch.get("deadline", 0)):
            return "pending"
        note = str(watch.get("note") or f"Timer '{name}' expired.")
        if not wake(f"[heartbeat: {name}] {note}"):
            return "pending"
        if watch.get("repeat"):
            watch["deadline"] = now + float(watch.get("seconds", 60))
            return "fired"
        return "remove"

    finding = runner(str(watch.get("command", "")), float(watch.get("timeout", COMMAND_TIMEOUT_SECONDS)))
    if not finding:
        return "pending"
    if wake(f"[heartbeat: {name}] {finding}"):
        return "remove" if watch.get("once") else "fired"
    return "pending"


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


def _scheduler_loop() -> None:
    logger.info("heartbeat: scheduler started")
    while not _scheduler_stop.is_set():
        now = time.time()
        with _state_lock:
            watches = _load_watches()
            due_watches = []
            for name, watch in watches.items():
                if not isinstance(watch, dict) or not watch.get("enabled", True):
                    continue
                if now < float(watch.get("next_run", 0)):
                    continue
                due_watches.append((name, dict(watch)))

        evaluated = []
        for name, original_watch in due_watches:
            watch = dict(original_watch)
            outcome = evaluate_watch(name, watch, now)
            evaluated.append((name, original_watch, watch, outcome))

        if evaluated:
            with _state_lock:
                watches = _load_watches()
                dirty = False
                for name, original_watch, watch, outcome in evaluated:
                    if watches.get(name) != original_watch:
                        continue
                    if outcome == "remove":
                        del watches[name]
                    else:
                        watch["next_run"] = now + float(watch.get("interval", 60))
                        watches[name] = watch
                    dirty = True
                if dirty:
                    _save_watches_locked(watches)
        _scheduler_stop.wait(POLL_SECONDS)
    logger.info("heartbeat: scheduler stopped")


def _try_acquire_scheduler_lock() -> bool:
    global _scheduler_lock_fd
    try:
        _scheduler_lock_fd = open(_state_dir() / LOCK_FILENAME, "w", encoding="utf-8")
        fcntl.flock(_scheduler_lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        if _scheduler_lock_fd:
            _scheduler_lock_fd.close()
            _scheduler_lock_fd = None
        return False


def _ensure_scheduler() -> None:
    global _scheduler_thread
    if not _owns_scheduler_lock:
        return
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _scheduler_stop.clear()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        name="heartbeat-scheduler",
        daemon=True,
    )
    _scheduler_thread.start()


def _claim_scheduler_if_available() -> bool:
    global _owns_scheduler_lock
    if _owns_scheduler_lock:
        _ensure_scheduler()
        return True
    if not _try_acquire_scheduler_lock():
        return False
    _owns_scheduler_lock = True
    _ensure_scheduler()
    return True


# ---------------------------------------------------------------------------
# Hook: capture gateway runner, loop, and routing
# ---------------------------------------------------------------------------


def _capture_gateway(**kwargs: Any) -> None:
    global _gateway_runner, _gateway_loop, _routing

    gateway = kwargs.get("gateway")
    if gateway is not None and _gateway_runner is None:
        _gateway_runner = gateway
        try:
            _gateway_loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                _gateway_loop = asyncio.get_event_loop()
            except RuntimeError:
                _gateway_loop = None
        _claim_scheduler_if_available()
        logger.info("heartbeat: captured gateway runner")

    if _pinned_routing is not None:
        _routing = _pinned_routing
        return None

    event = kwargs.get("event")
    source = getattr(event, "source", None)
    if source is None:
        return None
    if _routing is not None:
        return None
    platform = getattr(source, "platform", None)
    _routing = {
        "platform": platform.value if hasattr(platform, "value") else str(platform or ""),
        "chat_id": getattr(source, "chat_id", "") or "",
        "chat_name": getattr(source, "chat_name", None),
        "chat_type": getattr(source, "chat_type", "dm") or "dm",
        "thread_id": getattr(source, "thread_id", None),
    }
    return None


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


def heartbeat_watch_tool(
    name: str,
    command: str = "",
    interval: float = 60,
    seconds: float = 0,
    note: str = "",
    once: bool = False,
    repeat: bool = False,
) -> str:
    if not name or not name.strip():
        return json.dumps({"error": "name is required"})
    name = name.strip()
    now = time.time()

    if seconds and seconds > 0:
        watch: Dict[str, Any] = {
            "type": "timer",
            "deadline": now + float(seconds),
            "seconds": float(seconds),
            "interval": max(TIMER_MIN_INTERVAL, min(60, float(seconds) / 10)),
            "note": note or f"Timer '{name}' expired.",
            "repeat": bool(repeat),
            "enabled": True,
            "next_run": now,
        }
    else:
        if not command.strip():
            return json.dumps({"error": "command is required for command watches (or pass seconds>0 for a timer)"})
        if command.strip().split(maxsplit=1)[0].startswith("mcp__"):
            return json.dumps(
                {"error": "command must be a shell command, not an MCP tool name; use a real executable or script"}
            )
        watch = {
            "type": "command",
            "command": command,
            "interval": max(5, float(interval)),
            "once": bool(once),
            "note": note,
            "enabled": True,
            "next_run": now,
        }
    with _state_lock:
        watches = _load_watches()
        watches[name] = watch
        _save_watches_locked(watches)
    _ensure_scheduler()
    return json.dumps({"status": "watching", "name": name, "watch": watch})


def heartbeat_unwatch_tool(name: str) -> str:
    with _state_lock:
        watches = _load_watches()
        if name not in watches:
            return json.dumps({"error": f"watch '{name}' not found"})
        del watches[name]
        _save_watches_locked(watches)
    return json.dumps({"status": "removed", "name": name})


def heartbeat_list_tool() -> str:
    watches = _load_watches()
    now = time.time()
    out = []
    for name, watch in watches.items():
        entry = {
            "name": name,
            "type": watch.get("type", "command"),
            "enabled": watch.get("enabled", True),
            "interval": watch.get("interval"),
        }
        if watch.get("type") == "timer":
            entry["fires_in_seconds"] = max(0, int(float(watch.get("deadline", 0)) - now))
        out.append(entry)
    return json.dumps({"watches": out, "count": len(out)}, indent=2)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def _register_tools(register_tool: Callable[..., Any]) -> None:
    register_tool(
        name="heartbeat_watch",
        handler=lambda args, **kw: heartbeat_watch_tool(
            name=args.get("name", ""),
            command=args.get("command", ""),
            interval=args.get("interval", 60),
            seconds=args.get("seconds", 0),
            note=args.get("note", ""),
            once=args.get("once", False),
            repeat=args.get("repeat", False),
        ),
        schema={
            "name": "heartbeat_watch",
            "description": (
                "Watch anything that can produce a done. Either a one-shot timer "
                "(seconds>0, optionally with note) or a recurring check command "
                "(empty stdout = nothing; non-empty stdout = finding that wakes "
                "the agent in its main session)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Unique watch name."},
                    "command": {
                        "type": "string",
                        "description": "Shell check command for command watches. Prints the finding when done, nothing otherwise.",
                    },
                    "interval": {
                        "type": "number",
                        "description": "Seconds between command checks (default 60).",
                    },
                    "seconds": {
                        "type": "number",
                        "description": "One-shot timer duration in seconds. If > 0, creates a timer instead of a command watch.",
                    },
                    "note": {"type": "string", "description": "Optional custom wake text."},
                    "once": {
                        "type": "boolean",
                        "description": "Remove a command watch after its first fire (default false).",
                    },
                    "repeat": {
                        "type": "boolean",
                        "description": "Re-arm a timer after each fire instead of removing it (default false).",
                    },
                },
                "required": ["name"],
            },
        },
        toolset="heartbeat",
        description="Watch anything that can produce a done; wake the agent when it happens.",
        emoji="⏰",
        check_fn=lambda: True,
    )
    register_tool(
        name="heartbeat_unwatch",
        handler=lambda args, **kw: heartbeat_unwatch_tool(name=args.get("name", "")),
        schema={
            "name": "heartbeat_unwatch",
            "description": "Remove a heartbeat watch by name.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Watch name to remove."},
                },
                "required": ["name"],
            },
        },
        toolset="heartbeat",
        description="Remove a heartbeat watch.",
        emoji="🛑",
        check_fn=lambda: True,
    )
    register_tool(
        name="heartbeat_list",
        handler=lambda args, **kw: heartbeat_list_tool(),
        schema={
            "name": "heartbeat_list",
            "description": "List all heartbeat watches with their type, interval, and state.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
        toolset="heartbeat",
        description="List all heartbeat watches.",
        emoji="📋",
        check_fn=lambda: True,
    )


def _load_pinned_routing() -> Optional[Dict[str, Any]]:
    """Resolve an explicit wake target from config.yaml or env, if any.

    Keys under ``plugins.entries.heartbeat-hermes`` (or the matching env
    vars): ``deliver_platform``, ``deliver_chat_id``, ``deliver_chat_type``,
    ``deliver_thread_id``. When unset, the wake target is the first
    conversation the gateway routes through the plugin.
    """
    cfg: Dict[str, Any] = {}
    try:
        from hermes_cli.config import load_config

        raw = (load_config() or {}).get("plugins", {}).get("entries", {}).get("heartbeat-hermes", {})
        if isinstance(raw, dict):
            cfg = raw
    except Exception:
        cfg = {}

    platform = os.environ.get("HEARTBEAT_DELIVER_PLATFORM") or cfg.get("deliver_platform")
    chat_id = os.environ.get("HEARTBEAT_DELIVER_CHAT_ID") or cfg.get("deliver_chat_id")
    if not platform or not chat_id:
        return None
    return {
        "platform": str(platform),
        "chat_id": str(chat_id),
        "chat_name": cfg.get("deliver_chat_name"),
        "chat_type": str(cfg.get("deliver_chat_type", "dm")),
        "thread_id": cfg.get("deliver_thread_id"),
    }


def register(ctx: Any) -> None:
    """Register heartbeat tools and the gateway-capture hook."""
    global _pinned_routing, _routing
    _pinned_routing = _load_pinned_routing()
    if _pinned_routing is not None:
        _routing = _pinned_routing
    _register_tools(ctx.register_tool)
    ctx.register_hook("pre_gateway_dispatch", _capture_gateway)
    logger.info("heartbeat plugin registered")
