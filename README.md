# agent-bridge

Bidirectional blocking ping-pong MCP server for live discussions between **Antigravity** and **Claude Code**.

## Overview

`agent-bridge` lets Antigravity and an interactive Claude Code session debate plans back and forth until neither agent can find another improvement. Both sessions connect to `server.cjs` over MCP stdio and coordinate through an atomic room state file (`data/room-default.json`).

## Features

- **Single-call Send + Wait (`peer_exchange`)**: Delivers a proposal or critique to the peer agent, unlocks their pending wait call, and blocks until the peer replies.
- **Heartbeat Renewal (`peer_wait`)**: Waits in 45-second cycles and returns `status: "waiting_heartbeat"` when the peer is running long benchmarks or reading code, preventing MCP request timeouts.
- **Automatic Consensus (`stance: "agreed"`)**: When an agent agrees 100% with the peer's latest version with zero changes, both sides unlock with `status: "converged"` and present the final plan to the user.
- **Pause & Intervene Safety**: If you pause one agent to give private instructions in chat while the other agent sends a message, the unread message is preserved in the inbox and delivered before the next outgoing send.
- **Wrong-Turn & Mutual-Wait Deadlock Protection**: Immediately rejects `peer_wait` with `status: "your_turn_to_reply"` when an agent already received the peer's latest message and owes a reply, or `status: "mutual_wait_prevented"` when the peer is already listening on an idle/converged room.
- **Zero Dependencies**: Pure Node.js (`server.cjs`, `setup.cjs`).

## Installation

Run the installer to register `agent-bridge` in both `~/.gemini/config/mcp_config.json` (for Antigravity) and `~/.claude.json` (for Claude Code):

```bash
node setup.cjs
node setup.cjs --check
node setup.cjs --uninstall
node server.cjs --selftest
node server.cjs --status
node server.cjs --reset
```

## Environment Variables

- `AGENT_BRIDGE_DATA_DIR`: Override the room state directory (default: `./data`).
- `AGENT_BRIDGE_ROLE`: Default agent identity (`antigravity` | `claude`).
- `ANTIGRAVITY_CONFIG_DIR`: Override `~/.gemini/config` path in `setup.cjs`.
- `CLAUDE_CONFIG_FILE`: Override `~/.claude.json` path in `setup.cjs`.

## MCP Tools

1. `peer_exchange(message, stance, topic?, new_session?, wait_for_reply?, timeout_sec?)`
   - `stance`: `"propose"` | `"critique"` | `"agreed"`
2. `peer_wait(timeout_sec?)`
3. `peer_status(action?: "status" | "reset")`

## Typical Workflow

1. In one agent (for example Claude Code), say: *"Άκου στο bridge"* (calls `peer_wait`).
2. In the other agent (for example Antigravity), say: *"Σχεδίασε το πλάνο και συζήτησέ το με τον άλλον στο bridge μέχρι να συμφωνήσετε"* (calls `peer_exchange` with `stance: "propose"`).
3. Both agents critique points, run measurements if needed, print round summaries in their own chat windows, and stop automatically once `stance: "agreed"` is reached.
