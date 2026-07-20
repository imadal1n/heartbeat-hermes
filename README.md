# heartbeat-hermes

Generic wake-on-done watcher plugin for Hermes Agent.

Think of it as an egg timer for anything: start a task, get woken when it's
done. A watch can be a one-shot timer ("boil the egg for 8 minutes") or a
recurring check command (a session going busy→idle, a PID exiting, a build
finishing, a file appearing). When a watch fires, the agent is woken in its
main session through the gateway's own synthetic-event pipeline and decides
itself what to do — the plugin never posts to any chat.

## How it works

```
1. A watch fires (timer deadline reached, or check command prints a finding)
2. The plugin builds a synthetic internal MessageEvent with the finding
3. The event enters the gateway's normal dispatch, into the agent's session
4. The agent wakes, investigates with its own tools, answers if it matters
```

No deliveries, no bot messages, no chat noise from the system. The only
thing that ever appears in a conversation is the agent's own voice.

## Watches

| Type    | Behavior |
|---------|----------|
| `timer`   | One-shot. Fires once `seconds` have elapsed, then is removed. |
| `command` | Recurring. Runs a shell check command every `interval` seconds. Empty stdout = nothing; non-empty stdout = the finding. With `once: true`, removed after the first fire. |

The check command owns the "what does done mean" logic entirely: state
files, transitions, thresholds. The plugin only wakes.

## Tools

### `heartbeat_watch`

Create or replace a watch.

```
# One-shot timer
heartbeat_watch(name="tea", seconds=240, note="Tea is ready")

# Recurring check command (fires when the command prints something)
heartbeat_watch(
    name="build",
    command="test -f /tmp/build.done && echo 'build finished'",
    interval=30,
    once=true,
)
```

### `heartbeat_unwatch`

Remove a watch by name.

### `heartbeat_list`

List all watches with type, interval, and state.

## State

Watches live in `$HERMES_HOME/heartbeat/watches.json` and survive restarts.
The scheduler is guarded by a cross-process file lock, so only one process
runs it even if multiple Hermes surfaces load the plugin.

## Install

Copy the `heartbeat_hermes/` directory into `$HERMES_HOME/plugins/heartbeat-hermes/`
and add `heartbeat-hermes` to `plugins.enabled` in the Hermes config.

The plugin uses only the Python standard library at runtime.

## Validate

```bash
uv run pytest
```

## License

MIT. See `LICENSE`.
