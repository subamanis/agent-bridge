#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const SERVER_NAME = 'agent-bridge';
const SERVER_VERSION = '1.0.0';
const DEFAULT_ROOM = 'default';
const DEFAULT_WAIT_SEC = 45;
const MAX_WAIT_SEC = 120;
const POLL_INTERVAL_MS = 180;
const HEARTBEAT_UPDATE_MS = 4000;
const LOCK_STALE_MS = 3000;
const STALE_SESSION_MS = 30 * 60 * 1000;

const DATA_DIR = process.env.AGENT_BRIDGE_DATA_DIR
  ? path.resolve(process.env.AGENT_BRIDGE_DATA_DIR)
  : path.join(__dirname, 'data');
const ARCHIVE_DIR = path.join(DATA_DIR, 'archive');

function ensureDirs() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  if (!fs.existsSync(ARCHIVE_DIR)) {
    fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
  }
}

function sanitizeRoomName(room) {
  const raw = String(room || DEFAULT_ROOM).trim().toLowerCase();
  const cleaned = raw.replace(/[^a-z0-9_-]/g, '');
  return cleaned || DEFAULT_ROOM;
}

function normalizeAgentRole(val) {
  if (!val) return null;
  const s = String(val).trim().toLowerCase();
  if (s.includes('claude')) return 'claude';
  if (s.includes('antigravity') || s.includes('agy') || s.includes('gemini')) {
    return 'antigravity';
  }
  if (s === 'claude' || s === 'antigravity') return s;
  return null;
}

function oppositeRole(role) {
  return role === 'claude' ? 'antigravity' : 'claude';
}

function parseCliArgs(argv) {
  const opts = {
    agent: normalizeAgentRole(process.env.AGENT_BRIDGE_ROLE),
    room: DEFAULT_ROOM,
    cliMode: null,
    message: null,
    stance: 'propose',
    topic: null,
    newSession: false,
    waitForReply: true,
    timeoutSec: DEFAULT_WAIT_SEC,
  };

  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--agent' && argv[i + 1]) {
      opts.agent = normalizeAgentRole(argv[++i]) || opts.agent;
    } else if (arg.startsWith('--agent=')) {
      opts.agent = normalizeAgentRole(arg.slice('--agent='.length)) || opts.agent;
    } else if (arg === '--room' && argv[i + 1]) {
      opts.room = sanitizeRoomName(argv[++i]);
    } else if (arg.startsWith('--room=')) {
      opts.room = sanitizeRoomName(arg.slice('--room='.length));
    } else if (arg === '--status') {
      opts.cliMode = 'status';
    } else if (arg === '--reset') {
      opts.cliMode = 'reset';
    } else if (arg === '--wait') {
      opts.cliMode = 'wait';
    } else if (arg === '--send' && argv[i + 1]) {
      opts.cliMode = 'send';
      opts.message = argv[++i];
    } else if (arg === '--stance' && argv[i + 1]) {
      opts.stance = argv[++i];
    } else if (arg === '--topic' && argv[i + 1]) {
      opts.topic = argv[++i];
    } else if (arg === '--new-session') {
      opts.newSession = true;
    } else if (arg === '--no-wait') {
      opts.waitForReply = false;
    } else if (arg === '--timeout' && argv[i + 1]) {
      opts.timeoutSec = Number(argv[++i]) || DEFAULT_WAIT_SEC;
    } else if (arg === '--selftest') {
      opts.cliMode = 'selftest';
    } else if (arg === '--help' || arg === '-h') {
      opts.cliMode = 'help';
    }
  }
  return opts;
}

const cliOpts = parseCliArgs(process.argv);
let detectedSelfRole = cliOpts.agent;

function getSelfRole(explicitAgent) {
  const fromArg = normalizeAgentRole(explicitAgent);
  if (fromArg) return fromArg;
  if (detectedSelfRole) return detectedSelfRole;
  return 'antigravity';
}

function getRoomPaths(roomName) {
  const room = sanitizeRoomName(roomName);
  return {
    room,
    stateFile: path.join(DATA_DIR, `room-${room}.json`),
    lockFile: path.join(DATA_DIR, `room-${room}.lock`),
  };
}

function sleepMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function acquireLockSync(lockFile) {
  ensureDirs();
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    try {
      const fd = fs.openSync(lockFile, 'wx');
      fs.writeSync(fd, String(process.pid));
      fs.closeSync(fd);
      return true;
    } catch (err) {
      if (err && err.code === 'EEXIST') {
        try {
          const stat = fs.statSync(lockFile);
          if (Date.now() - stat.mtimeMs > LOCK_STALE_MS) {
            fs.unlinkSync(lockFile);
            continue;
          }
        } catch {}
        const waitUntil = Date.now() + 15;
        while (Date.now() < waitUntil) {
          // short spin under contention
        }
        continue;
      }
      throw err;
    }
  }
  try {
    fs.unlinkSync(lockFile);
  } catch {}
  const fd = fs.openSync(lockFile, 'wx');
  fs.writeSync(fd, String(process.pid));
  fs.closeSync(fd);
  return true;
}

function releaseLockSync(lockFile) {
  try {
    fs.unlinkSync(lockFile);
  } catch {}
}

function createFreshRoomState(roomName, topic) {
  const nowIso = new Date().toISOString();
  const nowMs = Date.now();
  return {
    room: sanitizeRoomName(roomName),
    sessionId: `sess-${nowMs}-${crypto.randomBytes(3).toString('hex')}`,
    topic: topic ? String(topic).trim() : '',
    status: 'idle',
    createdAt: nowIso,
    updatedAt: nowIso,
    updatedMs: nowMs,
    seq: 0,
    messages: [],
    cursors: {
      antigravity: 0,
      claude: 0,
    },
    presence: {
      antigravity: { state: 'idle', sinceMs: nowMs, updatedMs: nowMs, detail: 'Idle' },
      claude: { state: 'idle', sinceMs: nowMs, updatedMs: nowMs, detail: 'Idle' },
    },
  };
}

function readRoomStateRawSync(stateFile, roomName) {
  if (!fs.existsSync(stateFile)) {
    return createFreshRoomState(roomName, '');
  }
  try {
    const raw = fs.readFileSync(stateFile, 'utf-8').replace(/^\uFEFF/, '');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed.messages)) {
      return createFreshRoomState(roomName, '');
    }
    if (!parsed.cursors) parsed.cursors = { antigravity: 0, claude: 0 };
    if (!parsed.presence) {
      const nowMs = Date.now();
      parsed.presence = {
        antigravity: { state: 'idle', sinceMs: nowMs, updatedMs: nowMs, detail: 'Idle' },
        claude: { state: 'idle', sinceMs: nowMs, updatedMs: nowMs, detail: 'Idle' },
      };
    }
    return parsed;
  } catch {
    return createFreshRoomState(roomName, '');
  }
}

function writeRoomStateRawSync(stateFile, state) {
  ensureDirs();
  const tmpFile = `${stateFile}.tmp.${process.pid}.${Date.now()}`;
  fs.writeFileSync(tmpFile, JSON.stringify(state, null, 2), 'utf-8');
  fs.renameSync(tmpFile, stateFile);
}

function archiveRoomStateSync(state) {
  if (!state || !Array.isArray(state.messages) || state.messages.length === 0) {
    return null;
  }
  ensureDirs();
  const archiveFile = path.join(ARCHIVE_DIR, `${state.room}-${state.sessionId}.json`);
  try {
    fs.writeFileSync(archiveFile, JSON.stringify(state, null, 2), 'utf-8');
    return archiveFile;
  } catch {
    return null;
  }
}

function mutateRoomSync(roomName, mutatorFn) {
  const { room, stateFile, lockFile } = getRoomPaths(roomName);
  acquireLockSync(lockFile);
  try {
    const state = readRoomStateRawSync(stateFile, room);
    const result = mutatorFn(state);
    if (result && result.nextState) {
      writeRoomStateRawSync(stateFile, result.nextState);
    } else {
      writeRoomStateRawSync(stateFile, state);
    }
    return result;
  } finally {
    releaseLockSync(lockFile);
  }
}

function readRoomSync(roomName) {
  const { room, stateFile } = getRoomPaths(roomName);
  return readRoomStateRawSync(stateFile, room);
}

function getUnreadPeerMessages(state, selfRole) {
  const peerRole = oppositeRole(selfRole);
  const cursor = Number(state.cursors?.[selfRole] || 0);
  return state.messages.filter((m) => m.from === peerRole && m.seq > cursor);
}

function formatPresenceSummary(presenceObj) {
  if (!presenceObj) return 'unknown';
  const ageSec = Math.max(0, Math.round((Date.now() - (presenceObj.updatedMs || 0)) / 1000));
  return `${presenceObj.state} (${presenceObj.detail || 'no detail'}, updated ${ageSec}s ago)`;
}

function buildDeliveryPayload({ state, selfRole, peerRole, deliveredMessages, eventType, note }) {
  const latest = deliveredMessages[deliveredMessages.length - 1] || null;
  const isConverged = state.status === 'converged' || (latest && latest.stance === 'agreed');

  let instruction = '';
  if (eventType === 'unread_inbox_first') {
    instruction =
      `HOLD ON: ${peerRole} already sent message #${latest.seq} while you were not listening. ` +
      `Your outgoing message was NOT sent yet so you do not talk past each other. ` +
      `1) Summarize ${peerRole}'s message for the user in your chat. ` +
      `2) Reconcile their points with your analysis and any user instructions. ` +
      `3) Call peer_exchange again to send your unified response.`;
  } else if (isConverged) {
    instruction =
      `CONSENSUS REACHED: Both ${selfRole} and ${peerRole} have converged on the plan (stance: "agreed"). ` +
      `Stop calling peer_exchange/peer_wait and present the final unified plan clearly to the user.`;
  } else {
    instruction =
      `Received reply #${latest.seq} from ${peerRole} (stance: "${latest.stance}"). ` +
      `Next steps: ` +
      `1) First write a concise update in your chat window so the user can see what ${peerRole} argued and your assessment. ` +
      `2) Verify claims against the codebase and run measurements/benchmarks if needed. ` +
      `3) Reply via peer_exchange with stance: "critique" if you have ANY remaining objections or improvements, ` +
      `or stance: "agreed" ONLY if you agree 100% with zero further changes.`;
  }

  return {
    status: isConverged ? 'converged' : eventType || 'message_received',
    room: state.room,
    session_id: state.sessionId,
    topic: state.topic || '(untitled)',
    room_status: state.status,
    self: selfRole,
    peer: peerRole,
    total_messages: state.seq,
    peer_message: latest
      ? {
          seq: latest.seq,
          from: latest.from,
          stance: latest.stance,
          timestamp: latest.timestamp,
          content: latest.content,
        }
      : null,
    all_unread_delivered: deliveredMessages.length > 1 ? deliveredMessages : undefined,
    note: note || undefined,
    instruction,
  };
}

async function waitForPeerTurn({
  roomName,
  selfRole,
  peerRole,
  baselineSessionId,
  timeoutSec,
  sentSeq,
}) {
  const clampedSec = Math.min(MAX_WAIT_SEC, Math.max(5, Number(timeoutSec) || DEFAULT_WAIT_SEC));
  const deadlineMs = Date.now() + clampedSec * 1000;
  const { stateFile } = getRoomPaths(roomName);

  let lastHeartbeatWriteMs = Date.now();
  let lastObservedMtimeMs = 0;

  let watcher = null;
  let wakeResolver = null;
  const triggerWake = () => {
    if (wakeResolver) {
      const r = wakeResolver;
      wakeResolver = null;
      r();
    }
  };

  try {
    ensureDirs();
    watcher = fs.watch(DATA_DIR, (eventType, filename) => {
      if (!filename || filename.includes(`room-${sanitizeRoomName(roomName)}`)) {
        triggerWake();
      }
    });
  } catch {}

  try {
    while (Date.now() < deadlineMs) {
      let shouldInspect = false;
      try {
        if (fs.existsSync(stateFile)) {
          const stat = fs.statSync(stateFile);
          if (stat.mtimeMs !== lastObservedMtimeMs) {
            lastObservedMtimeMs = stat.mtimeMs;
            shouldInspect = true;
          }
        }
      } catch {
        shouldInspect = true;
      }

      if (shouldInspect) {
        const checkResult = mutateRoomSync(roomName, (state) => {
          const sessionChanged = baselineSessionId && state.sessionId !== baselineSessionId;
          const unread = getUnreadPeerMessages(state, selfRole);
          if (unread.length > 0) {
            const latestSeq = unread[unread.length - 1].seq;
            state.cursors[selfRole] = latestSeq;
            const nowMs = Date.now();
            const isConverged =
              state.status === 'converged' || unread[unread.length - 1].stance === 'agreed';
            state.presence[selfRole] = {
              state: isConverged ? 'idle' : 'working',
              sinceMs: nowMs,
              updatedMs: nowMs,
              detail: isConverged
                ? `Converged at #${latestSeq}`
                : `Received #${latestSeq} from ${peerRole}, working on reply`,
            };
            state.updatedMs = nowMs;
            state.updatedAt = new Date(nowMs).toISOString();
            return {
              delivered: true,
              payload: buildDeliveryPayload({
                state,
                selfRole,
                peerRole,
                deliveredMessages: unread,
                eventType: 'message_received',
                note: sessionChanged ? 'Peer started a new session.' : undefined,
              }),
            };
          }
          return { delivered: false };
        });

        if (checkResult && checkResult.delivered) {
          return checkResult.payload;
        }
      }

      const nowMs = Date.now();
      if (nowMs - lastHeartbeatWriteMs >= HEARTBEAT_UPDATE_MS) {
        lastHeartbeatWriteMs = nowMs;
        mutateRoomSync(roomName, (state) => {
          const prev = state.presence[selfRole] || { sinceMs: nowMs };
          state.presence[selfRole] = {
            state: 'waiting',
            sinceMs: prev.sinceMs || nowMs,
            updatedMs: nowMs,
            detail: sentSeq
              ? `Waiting for ${peerRole}'s reply to #${sentSeq}`
              : `Listening for ${peerRole}`,
          };
          return {};
        });
      }

      const remainingMs = deadlineMs - Date.now();
      if (remainingMs <= 0) break;

      await new Promise((resolve) => {
        wakeResolver = resolve;
        setTimeout(() => {
          if (wakeResolver === resolve) {
            wakeResolver = null;
            resolve();
          }
        }, Math.min(POLL_INTERVAL_MS, remainingMs));
      });
    }
  } finally {
    if (watcher) {
      try {
        watcher.close();
      } catch {}
    }
  }

  const finalState = readRoomSync(roomName);
  const peerPres = finalState.presence?.[peerRole];
  return {
    status: 'waiting_heartbeat',
    room: finalState.room,
    session_id: finalState.sessionId,
    self: selfRole,
    peer: peerRole,
    waited_sec: clampedSec,
    sent_seq: sentSeq || undefined,
    peer_presence: formatPresenceSummary(peerPres),
    instruction:
      `${peerRole} has not replied yet after ${clampedSec}s (${formatPresenceSummary(peerPres)}). ` +
      `They may still be running benchmarks, reading files, or waiting for user input. ` +
      `Call peer_wait() immediately without stopping your turn so you receive their reply as soon as they finish.`,
  };
}

async function handlePeerExchange(args) {
  const selfRole = getSelfRole(args.agent);
  const peerRole = oppositeRole(selfRole);
  const roomName = sanitizeRoomName(args.room || cliOpts.room);
  const message = String(args.message || '').trim();
  if (!message) {
    throw new Error('Parameter "message" is required and cannot be empty.');
  }

  let stance = String(args.stance || 'critique').trim().toLowerCase();
  if (!['propose', 'critique', 'agreed'].includes(stance)) {
    stance = 'critique';
  }

  const waitForReply = args.wait_for_reply !== false;
  const timeoutSec = args.timeout_sec || DEFAULT_WAIT_SEC;
  const explicitNewSession = Boolean(args.new_session);

  const sendOutcome = mutateRoomSync(roomName, (state) => {
    const nowMs = Date.now();
    const nowIso = new Date(nowMs).toISOString();
    const isStale = state.seq > 0 && nowMs - (state.updatedMs || 0) > STALE_SESSION_MS;

    if (explicitNewSession || (isStale && stance === 'propose')) {
      archiveRoomStateSync(state);
      const fresh = createFreshRoomState(roomName, args.topic || '');
      Object.assign(state, fresh);
    }

    if (args.topic && !state.topic) {
      state.topic = String(args.topic).trim();
    }

    const unreadFromPeer = getUnreadPeerMessages(state, selfRole);
    if (unreadFromPeer.length > 0 && !explicitNewSession) {
      const latestSeq = unreadFromPeer[unreadFromPeer.length - 1].seq;
      state.cursors[selfRole] = latestSeq;
      state.presence[selfRole] = {
        state: 'working',
        sinceMs: nowMs,
        updatedMs: nowMs,
        detail: `Read unread #${latestSeq} from ${peerRole} before sending`,
      };
      state.updatedMs = nowMs;
      state.updatedAt = nowIso;
      return {
        action: 'deliver_unread_first',
        payload: buildDeliveryPayload({
          state,
          selfRole,
          peerRole,
          deliveredMessages: unreadFromPeer,
          eventType: 'unread_inbox_first',
        }),
      };
    }

    const peerHasPriorMessages = state.messages.some((m) => m.from === peerRole);
    let effectiveStance = stance;
    let note = null;
    if (effectiveStance === 'agreed' && !peerHasPriorMessages) {
      effectiveStance = 'propose';
      note =
        'Downgraded stance from "agreed" to "propose" because peer has not reviewed or sent any message in this session yet.';
    }

    const nextSeq = state.seq + 1;
    state.seq = nextSeq;
    const msgObj = {
      seq: nextSeq,
      from: selfRole,
      to: peerRole,
      stance: effectiveStance,
      content: message,
      timestamp: nowIso,
    };
    state.messages.push(msgObj);
    state.cursors[selfRole] = nextSeq;
    state.updatedMs = nowMs;
    state.updatedAt = nowIso;

    if (effectiveStance === 'agreed') {
      state.status = 'converged';
      state.presence[selfRole] = {
        state: 'idle',
        sinceMs: nowMs,
        updatedMs: nowMs,
        detail: `Agreed at #${nextSeq}`,
      };
      return {
        action: 'converged_immediately',
        payload: {
          status: 'converged',
          room: state.room,
          session_id: state.sessionId,
          topic: state.topic || '(untitled)',
          self: selfRole,
          peer: peerRole,
          sent_seq: nextSeq,
          stance: effectiveStance,
          instruction:
            `CONSENSUS REACHED: You (${selfRole}) agreed with ${peerRole}'s latest proposal (#${nextSeq}). ` +
            `${peerRole} has been notified and unlocked. Stop calling bridge tools and present the final agreed plan to the user.`,
        },
      };
    }

    state.status = 'active';
    state.presence[selfRole] = {
      state: waitForReply ? 'waiting' : 'idle',
      sinceMs: nowMs,
      updatedMs: nowMs,
      detail: waitForReply
        ? `Sent #${nextSeq} (${effectiveStance}), waiting for ${peerRole}`
        : `Sent #${nextSeq} (${effectiveStance})`,
    };

    return {
      action: waitForReply ? 'wait_for_peer' : 'sent_no_wait',
      sessionId: state.sessionId,
      sentSeq: nextSeq,
      effectiveStance,
      note,
    };
  });

  if (
    sendOutcome.action === 'deliver_unread_first' ||
    sendOutcome.action === 'converged_immediately'
  ) {
    return sendOutcome.payload;
  }

  if (sendOutcome.action === 'sent_no_wait') {
    return {
      status: 'sent',
      room: roomName,
      session_id: sendOutcome.sessionId,
      self: selfRole,
      peer: peerRole,
      sent_seq: sendOutcome.sentSeq,
      stance: sendOutcome.effectiveStance,
      note: sendOutcome.note || undefined,
      instruction: `Message #${sendOutcome.sentSeq} queued for ${peerRole} without blocking. Call peer_wait() when you are ready to listen for their reply.`,
    };
  }

  return waitForPeerTurn({
    roomName,
    selfRole,
    peerRole,
    baselineSessionId: sendOutcome.sessionId,
    timeoutSec,
    sentSeq: sendOutcome.sentSeq,
  });
}

async function handlePeerWait(args) {
  const selfRole = getSelfRole(args.agent);
  const peerRole = oppositeRole(selfRole);
  const roomName = sanitizeRoomName(args.room || cliOpts.room);
  const timeoutSec = args.timeout_sec || DEFAULT_WAIT_SEC;

  const initialCheck = mutateRoomSync(roomName, (state) => {
    const nowMs = Date.now();
    const unread = getUnreadPeerMessages(state, selfRole);
    if (unread.length > 0) {
      const latestSeq = unread[unread.length - 1].seq;
      state.cursors[selfRole] = latestSeq;
      const isConverged =
        state.status === 'converged' || unread[unread.length - 1].stance === 'agreed';
      state.presence[selfRole] = {
        state: isConverged ? 'idle' : 'working',
        sinceMs: nowMs,
        updatedMs: nowMs,
        detail: isConverged
          ? `Converged at #${latestSeq}`
          : `Received #${latestSeq} from ${peerRole}, working on reply`,
      };
      state.updatedMs = nowMs;
      state.updatedAt = new Date(nowMs).toISOString();
      return {
        immediate: true,
        payload: buildDeliveryPayload({
          state,
          selfRole,
          peerRole,
          deliveredMessages: unread,
          eventType: 'message_received',
        }),
      };
    }

    state.presence[selfRole] = {
      state: 'waiting',
      sinceMs: nowMs,
      updatedMs: nowMs,
      detail: `Listening for ${peerRole}`,
    };
    return {
      immediate: false,
      baselineSessionId: state.sessionId,
    };
  });

  if (initialCheck.immediate) {
    return initialCheck.payload;
  }

  return waitForPeerTurn({
    roomName,
    selfRole,
    peerRole,
    baselineSessionId: initialCheck.baselineSessionId,
    timeoutSec,
    sentSeq: null,
  });
}

function handlePeerStatus(args) {
  const selfRole = getSelfRole(args.agent);
  const peerRole = oppositeRole(selfRole);
  const roomName = sanitizeRoomName(args.room || cliOpts.room);
  const action = String(args.action || 'status').trim().toLowerCase();

  if (action === 'reset') {
    let archivedPath = null;
    const fresh = mutateRoomSync(roomName, (state) => {
      archivedPath = archiveRoomStateSync(state);
      const nextState = createFreshRoomState(roomName, '');
      return { nextState };
    });
    return {
      status: 'reset_complete',
      room: roomName,
      archived_previous_to: archivedPath,
      new_session_id: fresh.nextState.sessionId,
      instruction: 'Room has been archived and reset. Ready for a new discussion.',
    };
  }

  const state = readRoomSync(roomName);
  const unreadCount = getUnreadPeerMessages(state, selfRole).length;
  return {
    status: 'ok',
    room: state.room,
    session_id: state.sessionId,
    topic: state.topic || '(untitled)',
    room_status: state.status,
    self: selfRole,
    peer: peerRole,
    seq: state.seq,
    unread_for_self: unreadCount,
    cursors: state.cursors,
    presence: {
      antigravity: formatPresenceSummary(state.presence?.antigravity),
      claude: formatPresenceSummary(state.presence?.claude),
    },
    messages: state.messages,
  };
}

const MCP_TOOLS = [
  {
    name: 'peer_exchange',
    description:
      'Send your plan, point-by-point critique, benchmark results, or final agreement to the peer agent (claude or antigravity) and automatically wait for their reply.\n\n' +
      'Debate & Planning Protocol:\n' +
      '1. Before calling peer_exchange, always output a concise summary of your findings or counter-arguments in your own chat window so the user can follow the live discussion and intervene if needed.\n' +
      '2. Be rigorous: verify claims against the codebase and run measurements/benchmarks when performance or behavior is in question. Never agree out of politeness.\n' +
      '3. Address points explicitly (e.g. "Point 1: Agree. Point 2: Disagree because...").\n' +
      '4. Set stance="propose" for an initial plan or major revision; stance="critique" whenever you have ANY objection, counter-proposal, or improvement; stance="agreed" ONLY when you have zero remaining objections or improvements to the peer\'s latest plan.\n' +
      '5. If peer_exchange returns status="waiting_heartbeat" (because the peer is running long measurements), immediately call peer_wait() to keep waiting.',
    inputSchema: {
      type: 'object',
      properties: {
        message: {
          type: 'string',
          description:
            'Your full message to the peer agent (plan proposal, numbered point-by-point critique, measurements, or final agreement summary).',
        },
        stance: {
          type: 'string',
          enum: ['propose', 'critique', 'agreed'],
          description:
            '"propose" for initial plan or major rework; "critique" when raising any objection, question, or improvement; "agreed" ONLY when you agree 100% with the peer\'s latest version with zero changes.',
        },
        topic: {
          type: 'string',
          description: 'Optional short title for the discussion topic.',
        },
        new_session: {
          type: 'boolean',
          description:
            'Set to true ONLY on the first message of a brand-new topic to archive previous messages and start a clean session.',
        },
        wait_for_reply: {
          type: 'boolean',
          description:
            'Whether to block and wait for the peer reply (default: true). Automatically false when stance="agreed" reaches consensus.',
        },
        timeout_sec: {
          type: 'number',
          description:
            'Seconds to wait per heartbeat cycle before returning status="waiting_heartbeat" (default: 45, max: 120).',
        },
        agent: {
          type: 'string',
          enum: ['antigravity', 'claude'],
          description:
            'Optional override for your agent identity (auto-detected from server args or MCP clientInfo by default).',
        },
      },
      required: ['message', 'stance'],
    },
  },
  {
    name: 'peer_wait',
    description:
      'Listen and wait for the next message from the peer agent (claude or antigravity).\n\n' +
      'When to use:\n' +
      '1. When the user tells you to listen on the bridge / wait for the other agent to send a plan.\n' +
      '2. Immediately after peer_exchange or peer_wait returns status="waiting_heartbeat" (meaning the peer is still analyzing code or running measurements).\n\n' +
      'When a message arrives: summarize the peer\'s points for the user in your chat window, perform any needed checks/measurements, and reply with peer_exchange (unless status="converged").',
    inputSchema: {
      type: 'object',
      properties: {
        timeout_sec: {
          type: 'number',
          description:
            'Seconds to wait before returning a waiting_heartbeat reminder (default: 45, max: 120).',
        },
        agent: {
          type: 'string',
          enum: ['antigravity', 'claude'],
          description: 'Optional override for your agent identity.',
        },
      },
    },
  },
  {
    name: 'peer_status',
    description:
      'Inspect the current bridge room state, peer presence (waiting, working, idle), and full message transcript, or reset/archive the room.',
    inputSchema: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['status', 'reset'],
          description:
            '"status" (default) returns presence and full transcript; "reset" archives the current transcript and clears the room.',
        },
      },
    },
  },
];

let useContentLengthFraming = false;

function sendJsonRpc(msgObj) {
  const json = JSON.stringify(msgObj);
  if (useContentLengthFraming) {
    const byteLen = Buffer.byteLength(json, 'utf-8');
    process.stdout.write(`Content-Length: ${byteLen}\r\n\r\n${json}`);
  } else {
    process.stdout.write(`${json}\n`);
  }
}

async function handleJsonRpcMessage(msg) {
  if (!msg || typeof msg !== 'object') return;

  const { id, method, params } = msg;
  const isNotification = id === undefined || id === null;

  try {
    if (method === 'initialize') {
      const clientName = params?.clientInfo?.name || '';
      if (!detectedSelfRole && clientName) {
        detectedSelfRole = normalizeAgentRole(clientName);
      }
      const requestedVersion = params?.protocolVersion || '2024-11-05';
      sendJsonRpc({
        jsonrpc: '2.0',
        id,
        result: {
          protocolVersion: requestedVersion,
          capabilities: {
            tools: {},
          },
          serverInfo: {
            name: SERVER_NAME,
            version: SERVER_VERSION,
          },
        },
      });
      return;
    }

    if (method === 'notifications/initialized' || method === 'initialized') {
      return;
    }

    if (method === 'ping') {
      if (!isNotification) {
        sendJsonRpc({ jsonrpc: '2.0', id, result: {} });
      }
      return;
    }

    if (method === 'tools/list') {
      sendJsonRpc({
        jsonrpc: '2.0',
        id,
        result: {
          tools: MCP_TOOLS,
        },
      });
      return;
    }

    if (method === 'resources/list') {
      sendJsonRpc({
        jsonrpc: '2.0',
        id,
        result: { resources: [] },
      });
      return;
    }

    if (method === 'prompts/list') {
      sendJsonRpc({
        jsonrpc: '2.0',
        id,
        result: { prompts: [] },
      });
      return;
    }

    if (method === 'tools/call') {
      const toolName = params?.name;
      const toolArgs = params?.arguments || {};
      let resultPayload;

      if (toolName === 'peer_exchange') {
        resultPayload = await handlePeerExchange(toolArgs);
      } else if (toolName === 'peer_wait') {
        resultPayload = await handlePeerWait(toolArgs);
      } else if (toolName === 'peer_status') {
        resultPayload = handlePeerStatus(toolArgs);
      } else {
        throw new Error(`Unknown tool: ${toolName}`);
      }

      sendJsonRpc({
        jsonrpc: '2.0',
        id,
        result: {
          content: [
            {
              type: 'text',
              text: JSON.stringify(resultPayload, null, 2),
            },
          ],
        },
      });
      return;
    }

    if (!isNotification) {
      sendJsonRpc({
        jsonrpc: '2.0',
        id,
        error: {
          code: -32601,
          message: `Method not found: ${method}`,
        },
      });
    }
  } catch (err) {
    if (!isNotification) {
      sendJsonRpc({
        jsonrpc: '2.0',
        id,
        result: {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify(
                {
                  status: 'error',
                  error: err instanceof Error ? err.message : String(err),
                },
                null,
                2
              ),
            },
          ],
        },
      });
    }
  }
}

function startMcpStdioServer() {
  let buffer = Buffer.alloc(0);

  process.stdin.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length > 0) {
      const preview = buffer.subarray(0, Math.min(buffer.length, 64)).toString('utf-8');
      if (/^Content-Length:/i.test(preview)) {
        useContentLengthFraming = true;
        const headerEnd = buffer.indexOf('\r\n\r\n');
        if (headerEnd === -1) break;
        const headerStr = buffer.subarray(0, headerEnd).toString('utf-8');
        const match = headerStr.match(/Content-Length:\s*(\d+)/i);
        if (!match) {
          buffer = buffer.subarray(headerEnd + 4);
          continue;
        }
        const contentLen = parseInt(match[1], 10);
        const bodyStart = headerEnd + 4;
        if (buffer.length < bodyStart + contentLen) break;
        const bodyStr = buffer.subarray(bodyStart, bodyStart + contentLen).toString('utf-8');
        buffer = buffer.subarray(bodyStart + contentLen);
        try {
          const parsed = JSON.parse(bodyStr);
          handleJsonRpcMessage(parsed);
        } catch {}
      } else {
        const nlIdx = buffer.indexOf(0x0a);
        if (nlIdx === -1) break;
        const line = buffer.subarray(0, nlIdx).toString('utf-8').trim();
        buffer = buffer.subarray(nlIdx + 1);
        if (!line) continue;
        try {
          const parsed = JSON.parse(line);
          handleJsonRpcMessage(parsed);
        } catch {}
      }
    }
  });

  process.stdin.on('end', () => {
    if (detectedSelfRole) {
      try {
        mutateRoomSync(cliOpts.room, (state) => {
          const nowMs = Date.now();
          if (state.presence?.[detectedSelfRole]?.state === 'waiting') {
            state.presence[detectedSelfRole] = {
              state: 'idle',
              sinceMs: nowMs,
              updatedMs: nowMs,
              detail: 'Disconnected',
            };
          }
          return {};
        });
      } catch {}
    }
    process.exit(0);
  });
}

async function runSelfTest() {
  const testRoom = `selftest_${ process.pid }`;
  const { stateFile } = getRoomPaths(testRoom);
  try {
    // 1. Reset room
    const r0 = handlePeerStatus({ room: testRoom, action: 'reset' });
    if (r0.status !== 'reset_complete') throw new Error('Reset failed');

    // 2. Start Claude waiting in background, while Antigravity proposes Plan v1
    const claudeWaitPromise = handlePeerWait({
      room: testRoom,
      agent: 'claude',
      timeout_sec: 10,
    });

    await sleepMs(250);

    // 3. Antigravity sends Plan v1 (propose) and waits for Claude's critique
    const agyExchange1Promise = handlePeerExchange({
      room: testRoom,
      agent: 'antigravity',
      topic: 'Cache Architecture',
      stance: 'propose',
      message: 'Plan v1: Point 1 use LRU, Point 2 use TTL 60s.',
      timeout_sec: 10,
    });

    // Claude should immediately wake up with Plan v1 (#1)
    const claudeRecv1 = await claudeWaitPromise;
    if (claudeRecv1.status !== 'message_received' || claudeRecv1.peer_message?.seq !== 1) {
      throw new Error(`Unexpected claudeRecv1: ${JSON.stringify(claudeRecv1)}`);
    }

    // 4. Claude critiques Point 2 (#2) and waits for Antigravity
    const claudeExchange1Promise = handlePeerExchange({
      room: testRoom,
      agent: 'claude',
      stance: 'critique',
      message: 'Point 1 agree. Point 2 disagree: TTL should be 300s based on benchmark.',
      timeout_sec: 10,
    });

    // Antigravity should immediately wake up with Claude's critique (#2)
    const agyRecv1 = await agyExchange1Promise;
    if (agyRecv1.status !== 'message_received' || agyRecv1.peer_message?.seq !== 2) {
      throw new Error(`Unexpected agyRecv1: ${JSON.stringify(agyRecv1)}`);
    }

    // 5. Antigravity updates plan (#3) and waits for Claude
    const agyExchange2Promise = handlePeerExchange({
      room: testRoom,
      agent: 'antigravity',
      stance: 'critique',
      message: 'Plan v2: Updated Point 2 to TTL 300s with invalidation hook.',
      timeout_sec: 10,
    });

    // Claude wakes up with Plan v2 (#3)
    const claudeRecv2 = await claudeExchange1Promise;
    if (claudeRecv2.status !== 'message_received' || claudeRecv2.peer_message?.seq !== 3) {
      throw new Error(`Unexpected claudeRecv2: ${JSON.stringify(claudeRecv2)}`);
    }

    // 6. Claude agrees 100% (stance: "agreed") -> both sides converge immediately!
    const claudeAgree = await handlePeerExchange({
      room: testRoom,
      agent: 'claude',
      stance: 'agreed',
      message: '100% agreed with Plan v2. Zero further improvements.',
      timeout_sec: 10,
    });
    const agyConverged = await agyExchange2Promise;

    if (claudeAgree.status !== 'converged' || agyConverged.status !== 'converged') {
      throw new Error(
        `Convergence failed: claude=${claudeAgree.status}, agy=${agyConverged.status}`
      );
    }

    // 7. Test unread inbox guard (user pauses Agent A while Agent B sends a message)
    await handlePeerExchange({
      room: testRoom,
      agent: 'claude',
      stance: 'critique',
      message: 'Wait, user asked to also check memory cap (#5).',
      wait_for_reply: false,
    });

    // Antigravity tries to send without having read #5 first -> gets unread_inbox_first!
    const unreadGuard = await handlePeerExchange({
      room: testRoom,
      agent: 'antigravity',
      stance: 'critique',
      message: 'My blind message before reading #5',
      wait_for_reply: false,
    });
    if (unreadGuard.status !== 'unread_inbox_first' || unreadGuard.peer_message?.seq !== 5) {
      throw new Error(`Unread guard failed: ${JSON.stringify(unreadGuard)}`);
    }

    // 8. Test heartbeat timeout when peer is running long measurements
    const heartbeatRes = await handlePeerWait({
      room: testRoom,
      agent: 'antigravity',
      timeout_sec: 1,
    });
    if (heartbeatRes.status !== 'waiting_heartbeat') {
      throw new Error(`Heartbeat test failed: ${JSON.stringify(heartbeatRes)}`);
    }

    console.log(
      JSON.stringify(
        {
          selftest: 'PASSED',
          rounds_tested: 5,
          convergence_verified: true,
          unread_inbox_guard_verified: true,
          heartbeat_verified: true,
        },
        null,
        2
      )
    );
  } finally {
    try {
      if (fs.existsSync(stateFile)) fs.unlinkSync(stateFile);
    } catch {}
  }
}

async function runCliMode() {
  if (cliOpts.cliMode === 'help') {
    console.log(
      [
        'Usage: node server.cjs [options]',
        '',
        'MCP Stdio Mode (default):',
        '  node server.cjs --agent antigravity',
        '  node server.cjs --agent claude',
        '',
        'CLI Helper / Diagnostics Mode:',
        '  node server.cjs --status [--room default]',
        '  node server.cjs --reset [--room default]',
        '  node server.cjs --wait --agent <antigravity|claude> [--timeout 45]',
        '  node server.cjs --send "<msg>" --agent <antigravity|claude> [--stance propose|critique|agreed] [--no-wait]',
        '  node server.cjs --selftest',
      ].join('\n')
    );
    return;
  }

  if (cliOpts.cliMode === 'selftest') {
    await runSelfTest();
    return;
  }

  if (cliOpts.cliMode === 'status') {
    console.log(JSON.stringify(handlePeerStatus({ room: cliOpts.room, action: 'status' }), null, 2));
    return;
  }

  if (cliOpts.cliMode === 'reset') {
    console.log(JSON.stringify(handlePeerStatus({ room: cliOpts.room, action: 'reset' }), null, 2));
    return;
  }

  if (cliOpts.cliMode === 'wait') {
    const res = await handlePeerWait({
      room: cliOpts.room,
      agent: cliOpts.agent,
      timeout_sec: cliOpts.timeoutSec,
    });
    console.log(JSON.stringify(res, null, 2));
    return;
  }

  if (cliOpts.cliMode === 'send') {
    const res = await handlePeerExchange({
      room: cliOpts.room,
      agent: cliOpts.agent,
      message: cliOpts.message,
      stance: cliOpts.stance,
      topic: cliOpts.topic,
      new_session: cliOpts.newSession,
      wait_for_reply: cliOpts.waitForReply,
      timeout_sec: cliOpts.timeoutSec,
    });
    console.log(JSON.stringify(res, null, 2));
  }
}

if (cliOpts.cliMode) {
  runCliMode().catch((err) => {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
} else {
  startMcpStdioServer();
}
