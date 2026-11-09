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

let dirsEnsured = false;
function ensureDirs() {
  if (!dirsEnsured) {
    fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
    dirsEnsured = true;
  }
}

function sanitizeRoomName(room) {
  const cleaned = String(room || DEFAULT_ROOM)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '');
  return cleaned || DEFAULT_ROOM;
}

function normalizeAgentRole(val) {
  if (!val) return null;
  const s = String(val).trim().toLowerCase();
  if (s.includes('claude')) return 'claude';
  if (s.includes('antigravity') || s.includes('agy') || s.includes('gemini')) {
    return 'antigravity';
  }
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
  };

  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--agent' || arg.startsWith('--agent=')) {
      const val = arg === '--agent' ? argv[++i] : arg.slice('--agent='.length);
      const role = val && !val.startsWith('-') ? normalizeAgentRole(val) : null;
      if (!role) throw new Error(`Invalid --agent value: ${val ?? ''}`);
      opts.agent = role;
    } else if (arg === '--room' || arg.startsWith('--room=')) {
      const val = arg === '--room' ? argv[++i] : arg.slice('--room='.length);
      if (!val || val.startsWith('-')) throw new Error(`Invalid --room value: ${val ?? ''}`);
      opts.room = sanitizeRoomName(val);
    } else if (arg === '--status' || arg === '--reset' || arg === '--selftest') {
      opts.cliMode = arg.slice(2);
    } else if (arg === '--help' || arg === '-h') {
      opts.cliMode = 'help';
    } else {
      throw new Error(`Unknown argument: ${arg}. Run "node server.cjs --help" for usage.`);
    }
  }
  return opts;
}

const cliOpts = parseCliArgs(require.main === module ? process.argv : []);
let detectedSelfRole = cliOpts.agent;
let hasActivePresence = false;

function resolveContext(args = {}) {
  const selfRole = normalizeAgentRole(args.agent) || (detectedSelfRole ||= 'antigravity');
  return {
    selfRole,
    peerRole: oppositeRole(selfRole),
    room: sanitizeRoomName(args.room || cliOpts.room),
  };
}

function normalizeTimeoutSec(val) {
  if (typeof val !== 'number' || !Number.isFinite(val)) return DEFAULT_WAIT_SEC;
  return Math.min(MAX_WAIT_SEC, Math.max(1, Math.round(val)));
}

function getRoomPaths(roomName) {
  const room = sanitizeRoomName(roomName);
  return {
    room,
    stateFile: path.join(DATA_DIR, `room-${room}.json`),
    lockFile: path.join(DATA_DIR, `room-${room}.lock`),
  };
}

const SLEEP_VIEW = new Int32Array(new SharedArrayBuffer(4));
function sleepSync(ms) {
  Atomics.wait(SLEEP_VIEW, 0, 0, ms);
}

function isTransientFsError(err) {
  const code = err && err.code;
  return code === 'EPERM' || code === 'EACCES' || code === 'EBUSY';
}

let tmpWriteCounter = 0;
let waitTokenCounter = 0;

function nextWaitToken() {
  return `${process.pid}:${Date.now()}:${++waitTokenCounter}`;
}

function acquireLockSync(lockFile) {
  ensureDirs();
  const token = `${process.pid}:${++tmpWriteCounter}`;
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    try {
      fs.writeFileSync(lockFile, token, { flag: 'wx' });
      return token;
    } catch (err) {
      if (err.code === 'EEXIST' || isTransientFsError(err)) {
        try {
          if (Date.now() - fs.statSync(lockFile).mtimeMs > LOCK_STALE_MS) {
            const staleTmp = `${lockFile}.stale.${process.pid}.${++tmpWriteCounter}`;
            try {
              fs.renameSync(lockFile, staleTmp);
              fs.unlinkSync(staleTmp);
            } catch {
              try {
                fs.unlinkSync(lockFile);
              } catch {}
            }
            continue;
          }
        } catch (statErr) {
          if (statErr.code === 'ENOENT') continue;
        }
        sleepSync(15);
        continue;
      }
      throw err;
    }
  }
  try {
    fs.writeFileSync(lockFile, token, { flag: 'w' });
  } catch {}
  return token;
}

function releaseLockSync(lockFile, token) {
  try {
    if (fs.readFileSync(lockFile, 'utf-8') === token) {
      fs.unlinkSync(lockFile);
    }
  } catch {}
}

function readJsonSafe(filePath) {
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const raw = fs.readFileSync(filePath, 'utf-8');
      const text = (raw.charCodeAt(0) === 0xfeff ? raw.slice(1) : raw).trim();
      if (!text) return {};
      const parsed = JSON.parse(text);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch (err) {
      if (err.code === 'ENOENT') return {};
      if ((isTransientFsError(err) || err instanceof SyntaxError) && attempt < 4) {
        sleepSync(12 * (attempt + 1));
        continue;
      }
      throw err;
    }
  }
}

function writeJsonAtomic(filePath, obj) {
  const dir = path.dirname(filePath);
  if (dir === DATA_DIR) {
    ensureDirs();
  } else {
    fs.mkdirSync(dir, { recursive: true });
  }
  const tmpFile = `${filePath}.tmp.${process.pid}.${++tmpWriteCounter}`;
  let renamed = false;
  try {
    fs.writeFileSync(tmpFile, JSON.stringify(obj, null, 2) + '\n', 'utf-8');
    for (let attempt = 0; attempt < 6; attempt++) {
      try {
        fs.renameSync(tmpFile, filePath);
        renamed = true;
        return;
      } catch (err) {
        if (isTransientFsError(err)) {
          if (attempt < 5) {
            sleepSync(12 * (attempt + 1));
            continue;
          }
          if (process.platform === 'win32') {
            fs.copyFileSync(tmpFile, filePath);
            return;
          }
        }
        throw err;
      }
    }
  } finally {
    if (!renamed) {
      try {
        fs.unlinkSync(tmpFile);
      } catch {}
    }
  }
}

function setPresence(
  state,
  role,
  nextState,
  detail,
  nowMs = Date.now(),
  preserveSince = false,
  waitToken = undefined,
  standby = undefined
) {
  if (role === detectedSelfRole) {
    hasActivePresence = nextState !== 'idle';
  }
  const prevSince = state.presence[role]?.sinceMs;
  const entry = {
    state: nextState,
    sinceMs: preserveSince && prevSince ? prevSince : nowMs,
    updatedMs: nowMs,
    detail,
  };
  if (nextState === 'waiting' && waitToken) {
    entry.waitToken = waitToken;
  }
  if (nextState === 'waiting' && standby) {
    entry.standby = true;
  }
  state.presence[role] = entry;
}

function waitingDetail(peerRole, sentSeq) {
  return sentSeq
    ? `Waiting for ${peerRole}'s reply to #${sentSeq}`
    : `Listening for ${peerRole}`;
}

function isFreshWaitingPresence(
  p,
  detail,
  nowMs,
  maxAgeMs = HEARTBEAT_UPDATE_MS,
  standby = false
) {
  return Boolean(
    p &&
      p.state === 'waiting' &&
      p.detail === detail &&
      Boolean(p.standby) === Boolean(standby) &&
      nowMs - (p.updatedMs || 0) < maxAgeMs
  );
}

const STANDBY_LISTENER_HINT =
  'To stay connected for the next bridge topic while keeping your main chat turn free to talk with the user, spawn a background listener subagent calling peer_wait(standby=true) before ending your turn.';

function buildConvergedPayload(state, selfRole, peerRole, seq, instruction) {
  return {
    status: 'converged',
    room: state.room,
    session_id: state.sessionId,
    topic: state.topic || '(untitled)',
    self: selfRole,
    peer: peerRole,
    sent_seq: seq,
    stance: 'agreed',
    instruction,
  };
}

function checkMutualWaitGuard(state, selfRole, peerRole, sentSeq, nowMs, standby = false) {
  const peerPres = state.presence[peerRole];
  if (
    sentSeq ||
    state.status === 'active' ||
    state.status === 'converged' ||
    standby ||
    peerPres?.standby ||
    peerPres?.state !== 'waiting' ||
    nowMs - (peerPres.updatedMs || 0) >= HEARTBEAT_UPDATE_MS * 3
  ) {
    return null;
  }
  const wasWaiting = state.presence[selfRole]?.state === 'waiting';
  if (wasWaiting) {
    setPresence(state, selfRole, 'idle', 'Idle', nowMs);
  }
  const peerSummary = formatPresenceSummary(peerPres);
  return {
    skipWrite: !wasWaiting,
    payload: {
      status: 'mutual_wait_prevented',
      room: state.room,
      session_id: state.sessionId,
      self: selfRole,
      peer: peerRole,
      peer_presence: peerSummary,
      instruction:
        `DEADLOCK PREVENTED: ${peerRole} is ALREADY waiting and listening for you (${peerSummary}). ` +
        `Both main agents cannot block in foreground peer_wait() at the same time on an idle room when no message is in flight. ` +
        `Do NOT call foreground peer_wait() again (use standby=true only from a background listener subagent). Either send the next message/proposal via peer_exchange() when your work finishes, or ask the user which agent should propose first.`,
    },
  };
}

function createFreshRoomState(room) {
  const nowMs = Date.now();
  const nowIso = new Date(nowMs).toISOString();
  return {
    room,
    sessionId: `sess-${nowMs}-${crypto.randomBytes(3).toString('hex')}`,
    topic: '',
    status: 'idle',
    createdAt: nowIso,
    updatedAt: nowIso,
    updatedMs: nowMs,
    seq: 0,
    messages: [],
    cursors: { antigravity: 0, claude: 0 },
    presence: {
      antigravity: { state: 'idle', sinceMs: nowMs, updatedMs: nowMs, detail: 'Idle' },
      claude: { state: 'idle', sinceMs: nowMs, updatedMs: nowMs, detail: 'Idle' },
    },
  };
}

function readRoomStateRawSync(stateFile, room) {
  try {
    const parsed = readJsonSafe(stateFile);
    if (!Array.isArray(parsed.messages)) {
      return createFreshRoomState(room);
    }
    parsed.cursors ||= { antigravity: 0, claude: 0 };
    parsed.presence ||= {};
    return parsed;
  } catch {
    ensureDirs();
    const corruptPath = path.join(ARCHIVE_DIR, `corrupt-${room}-${Date.now()}.json`);
    try {
      fs.renameSync(stateFile, corruptPath);
    } catch {
      try {
        fs.copyFileSync(stateFile, corruptPath);
        fs.unlinkSync(stateFile);
      } catch {}
    }
    return createFreshRoomState(room);
  }
}

function archiveRoomStateSync(state) {
  if (state.messages.length === 0) return null;
  ensureDirs();
  const archiveFile = path.join(ARCHIVE_DIR, `${state.room}-${state.sessionId}.json`);
  try {
    fs.writeFileSync(archiveFile, JSON.stringify(state, null, 2), 'utf-8');
    return archiveFile;
  } catch {
    return null;
  }
}

function resetRoomStateInPlace(state, room) {
  const archivedPath = archiveRoomStateSync(state);
  const prevPresence = state.presence;
  const fresh = createFreshRoomState(room);
  for (const role of ['antigravity', 'claude']) {
    const p = prevPresence[role];
    if (
      isFreshWaitingPresence(
        p,
        waitingDetail(oppositeRole(role), null),
        fresh.updatedMs,
        HEARTBEAT_UPDATE_MS * 3,
        Boolean(p?.standby)
      )
    ) {
      fresh.presence[role] = p;
    }
  }
  Object.assign(state, fresh);
  return archivedPath;
}

const activeWaits = new Map();

function mutateRoomSync(roomName, mutatorFn) {
  const { room, stateFile, lockFile } = getRoomPaths(roomName);
  const token = acquireLockSync(lockFile);
  let wrote = false;
  let result;
  try {
    const state = readRoomStateRawSync(stateFile, room);
    result = mutatorFn(state) || {};
    if (!result.skipWrite) {
      writeJsonAtomic(stateFile, state);
      wrote = true;
    }
    if (!result.payload && (wrote || result.baselineSessionId)) {
      try {
        const st = fs.statSync(stateFile);
        result._stat = { mtimeMs: st.mtimeMs, size: st.size };
      } catch {}
    }
  } finally {
    releaseLockSync(lockFile, token);
  }
  if (wrote) {
    for (const role of ['antigravity', 'claude']) {
      activeWaits.get(`${room}:${role}`)?.wake();
    }
  }
  return result;
}

function getUnreadPeerMessages(state, selfRole) {
  const peerRole = oppositeRole(selfRole);
  const cursor = Number(state.cursors[selfRole] || 0);
  return state.messages.filter((m) => m.from === peerRole && m.seq > cursor);
}

function formatPresenceSummary(presenceObj) {
  if (!presenceObj) return 'unknown';
  const ageSec = Math.max(0, Math.round((Date.now() - (presenceObj.updatedMs || 0)) / 1000));
  return `${presenceObj.state} (${presenceObj.detail || 'no detail'}, updated ${ageSec}s ago)`;
}

function consumeUnreadPeerMessages(state, selfRole, eventType = 'message_received', note) {
  const unread = getUnreadPeerMessages(state, selfRole);
  if (unread.length === 0) return null;

  const peerRole = oppositeRole(selfRole);
  const latest = unread[unread.length - 1];
  const latestSeq = latest.seq;
  const nowMs = Date.now();
  const isConverged = state.status === 'converged' || latest.stance === 'agreed';
  const unreadFirst = eventType === 'unread_inbox_first';
  state.cursors[selfRole] = latestSeq;

  setPresence(
    state,
    selfRole,
    isConverged ? 'idle' : 'working',
    isConverged
      ? `Converged at #${latestSeq}`
      : unreadFirst
        ? `Read unread #${latestSeq} from ${peerRole} before sending`
        : `Received #${latestSeq} from ${peerRole}, working on reply`,
    nowMs
  );

  state.updatedMs = nowMs;
  state.updatedAt = new Date(nowMs).toISOString();

  let instruction;
  if (unreadFirst) {
    instruction =
      `HOLD ON: ${peerRole} already sent message #${latestSeq} (stance: "${latest.stance}") while you were not listening. ` +
      `Your outgoing message was NOT sent yet so you do not talk past each other. ` +
      `1) Summarize ${peerRole}'s message for the user in your chat. ` +
      (isConverged
        ? `2) If you also agree 100% with zero further changes, present the final plan to the user (${STANDBY_LISTENER_HINT}); otherwise call peer_exchange again to send your response.`
        : `2) Reconcile their points with your analysis and any user instructions. ` +
          `3) Call peer_exchange again to send your unified response.`);
  } else if (isConverged) {
    instruction =
      `CONSENSUS REACHED: Both ${selfRole} and ${peerRole} have converged on the plan (stance: "agreed"). ` +
      `Present the final unified plan clearly to the user. ${STANDBY_LISTENER_HINT}`;
  } else {
    instruction =
      `Received reply #${latestSeq} from ${peerRole} (stance: "${latest.stance}"). ` +
      `Next steps: ` +
      `1) First write a concise update in your chat window so the user can see what ${peerRole} argued and your assessment. ` +
      `2) Verify claims against the codebase and run measurements/benchmarks if needed. ` +
      `3) Reply via peer_exchange with stance: "critique" if you have ANY remaining objections or improvements, ` +
      `or stance: "agreed" ONLY if you agree 100% with zero further changes.`;
  }

  return {
    status: unreadFirst ? 'unread_inbox_first' : isConverged ? 'converged' : eventType,
    room: state.room,
    session_id: state.sessionId,
    topic: state.topic || '(untitled)',
    room_status: state.status,
    self: selfRole,
    peer: peerRole,
    total_messages: state.seq,
    peer_message: {
      seq: latestSeq,
      from: latest.from,
      stance: latest.stance,
      timestamp: latest.timestamp,
      content: latest.content,
    },
    all_unread_delivered: unread.length > 1 ? unread : undefined,
    note: note || undefined,
    instruction,
  };
}

function cancelActiveWait(room, selfRole, reason = 'superseded') {
  const key = `${room}:${selfRole}`;
  const ctrl = activeWaits.get(key);
  if (!ctrl) return;

  ctrl.cancelled = true;
  ctrl.cancelReason = reason;
  activeWaits.delete(key);
  ctrl.wake();

  if (reason === 'rpc_cancelled') {
    try {
      mutateRoomSync(room, (state) => {
        if (state.presence[selfRole]?.state === 'waiting') {
          setPresence(state, selfRole, 'idle', 'Wait cancelled', Date.now());
          return {};
        }
        return { skipWrite: true };
      });
    } catch {}
  }
}

function cancelWaitByRequestId(requestId) {
  if (requestId == null) return;
  for (const ctrl of activeWaits.values()) {
    if (ctrl.requestId != null && String(ctrl.requestId) === String(requestId)) {
      cancelActiveWait(ctrl.room, ctrl.selfRole, 'rpc_cancelled');
      return;
    }
  }
}

async function waitForPeerTurn({
  room,
  selfRole,
  peerRole,
  baselineSessionId,
  timeoutSec,
  sentSeq,
  hadActiveSession,
  requestId,
  _stat,
  lastHeartbeatMs,
  peerPresence,
  note,
  waitToken,
  standby,
}) {
  const clampedSec = normalizeTimeoutSec(timeoutSec);
  const deadlineMs = Date.now() + clampedSec * 1000;
  const { stateFile } = getRoomPaths(room);
  let currentSessionId = baselineSessionId;
  let latestPeerPresence = peerPresence;

  let woken = false;
  let wakeResolver = null;
  const triggerWake = () => {
    woken = true;
    if (wakeResolver) wakeResolver();
  };

  const waitCtrl = {
    room,
    selfRole,
    requestId,
    cancelled: false,
    cancelReason: null,
    wake: triggerWake,
  };
  const agentKey = `${room}:${selfRole}`;
  activeWaits.set(agentKey, waitCtrl);

  let lastHeartbeatWriteMs = lastHeartbeatMs || Date.now();
  let lastMtime = _stat?.mtimeMs || 0;
  let lastSize = _stat?.size ?? -1;
  let watcher = null;

  try {
    watcher = fs.watch(DATA_DIR, (_eventType, filename) => {
      if (!filename || filename === `room-${room}.json`) {
        triggerWake();
      }
    });
    watcher.on('error', () => {});
  } catch {}

  try {
    while (!waitCtrl.cancelled) {
      let fileChanged = false;
      let currentStat = null;
      try {
        currentStat = fs.statSync(stateFile);
        if (currentStat.mtimeMs !== lastMtime || currentStat.size !== lastSize) {
          fileChanged = true;
        }
      } catch (err) {
        if (err.code !== 'ENOENT' || lastMtime !== 0) {
          lastMtime = 0;
          lastSize = -1;
          fileChanged = true;
        }
      }

      const nowMs = Date.now();
      const heartbeatDue =
        nowMs < deadlineMs && nowMs - lastHeartbeatWriteMs >= HEARTBEAT_UPDATE_MS;

      if (fileChanged || woken || heartbeatDue) {
        woken = false;
        const checkResult = mutateRoomSync(room, (state) => {
          latestPeerPresence = state.presence[peerRole];
          const changed = state.sessionId !== currentSessionId;
          const combinedNote =
            [note, changed ? 'Peer started a new session.' : null].filter(Boolean).join(' ') ||
            undefined;
          const payload = consumeUnreadPeerMessages(
            state,
            selfRole,
            'message_received',
            combinedNote
          );
          if (payload) {
            return { payload };
          }

          if (changed) {
            if ((sentSeq || hadActiveSession) && state.seq === 0) {
              setPresence(state, selfRole, 'idle', 'Room reset while waiting', nowMs);
              return {
                payload: {
                  status: 'session_reset',
                  room: state.room,
                  session_id: state.sessionId,
                  self: selfRole,
                  peer: peerRole,
                  note: note || undefined,
                  instruction:
                    'The bridge room was reset while waiting for a reply. Stop waiting and ask the user how to proceed.',
                },
              };
            }
            currentSessionId = state.sessionId;
          }

          const selfPres = state.presence[selfRole];
          if (
            waitToken &&
            selfPres?.waitToken !== waitToken &&
            (selfPres?.state !== 'waiting' || selfPres?.waitToken != null)
          ) {
            return {
              skipWrite: true,
              payload: {
                status: 'cancelled',
                room: state.room,
                self: selfRole,
                peer: peerRole,
                reason: 'superseded',
                instruction: 'Wait was cancelled or superseded by a newer request.',
              },
            };
          }

          const mutualGuard = checkMutualWaitGuard(
            state,
            selfRole,
            peerRole,
            sentSeq,
            nowMs,
            standby
          );
          if (mutualGuard) {
            return mutualGuard;
          }

          if (heartbeatDue || state.presence[selfRole]?.state !== 'waiting') {
            lastHeartbeatWriteMs = nowMs;
            setPresence(
              state,
              selfRole,
              'waiting',
              waitingDetail(peerRole, sentSeq),
              nowMs,
              true,
              waitToken,
              standby
            );
            return {};
          }

          return { skipWrite: true };
        });

        const nextStat = checkResult._stat || currentStat;
        if (nextStat) {
          lastMtime = nextStat.mtimeMs;
          lastSize = nextStat.size;
        }
        if (checkResult.payload) {
          return checkResult.payload;
        }
      }

      const remainingMs = deadlineMs - Date.now();
      if (remainingMs <= 0) break;

      await new Promise((resolve) => {
        const timer = setTimeout(() => {
          wakeResolver = null;
          resolve();
        }, Math.min(POLL_INTERVAL_MS, remainingMs));
        wakeResolver = () => {
          clearTimeout(timer);
          wakeResolver = null;
          resolve();
        };
      });
    }
  } finally {
    if (activeWaits.get(agentKey) === waitCtrl) {
      activeWaits.delete(agentKey);
    }
    if (watcher) {
      try {
        watcher.close();
      } catch {}
    }
  }

  if (waitCtrl.cancelled) {
    return {
      status: 'cancelled',
      room,
      self: selfRole,
      peer: peerRole,
      reason: waitCtrl.cancelReason,
      instruction: 'Wait was cancelled or superseded by a newer request.',
    };
  }

  const peerPresSummary = formatPresenceSummary(latestPeerPresence);
  return {
    status: 'waiting_heartbeat',
    room,
    session_id: currentSessionId,
    self: selfRole,
    peer: peerRole,
    waited_sec: clampedSec,
    sent_seq: sentSeq || undefined,
    standby: standby || undefined,
    note: note || undefined,
    peer_presence: peerPresSummary,
    instruction:
      `${peerRole} has not replied yet after ${clampedSec}s (${peerPresSummary}). ` +
      `They may still be running benchmarks, reading files, or waiting for user input. ` +
      `Call ${standby ? 'peer_wait(standby=true)' : 'peer_wait()'} immediately without stopping your turn so you receive their reply as soon as they finish.`,
  };
}

async function handlePeerExchange(args = {}, requestId) {
  const { selfRole, peerRole, room } = resolveContext(args);
  const message = String(args.message || '').trim();
  if (!message) {
    throw new Error('Parameter "message" is required and cannot be empty.');
  }

  let stance = String(args.stance || 'critique').trim().toLowerCase();
  if (!['propose', 'critique', 'agreed'].includes(stance)) {
    stance = 'critique';
  }

  const waitForReply = args.wait_for_reply !== false;
  const timeoutSec = normalizeTimeoutSec(args.timeout_sec);
  const explicitNewSession = Boolean(args.new_session);
  const trimmedTopic = String(args.topic || '').trim();

  cancelActiveWait(room, selfRole);

  const sendOutcome = mutateRoomSync(room, (state) => {
    const nowMs = Date.now();
    const nowIso = new Date(nowMs).toISOString();
    const isStale = state.seq > 0 && nowMs - (state.updatedMs || 0) > STALE_SESSION_MS;
    const hasNewTopic = Boolean(trimmedTopic && state.topic && trimmedTopic !== state.topic);

    if (!explicitNewSession) {
      const unreadPayload = consumeUnreadPeerMessages(state, selfRole, 'unread_inbox_first');
      if (unreadPayload) {
        return { payload: unreadPayload };
      }

      if (state.status === 'converged' && stance === 'agreed' && !isStale && !hasNewTopic) {
        const wasIdle = state.presence[selfRole]?.state === 'idle';
        if (!wasIdle) setPresence(state, selfRole, 'idle', `Agreed at #${state.seq}`, nowMs);
        return {
          skipWrite: wasIdle,
          payload: buildConvergedPayload(
            state,
            selfRole,
            peerRole,
            state.seq,
            `CONSENSUS ALREADY REACHED at #${state.seq}: Both ${selfRole} and ${peerRole} have converged on the plan. ` +
              `Present the final agreed plan to the user. ${STANDBY_LISTENER_HINT}`
          ),
        };
      }
    }

    if (
      explicitNewSession ||
      (isStale && (stance === 'propose' || getUnreadPeerMessages(state, peerRole).length === 0)) ||
      (state.status === 'converged' && (stance === 'propose' || hasNewTopic))
    ) {
      resetRoomStateInPlace(state, room);
    }

    if (trimmedTopic && (stance === 'propose' || !state.topic)) {
      state.topic = trimmedTopic;
    }

    const lastMsg = state.messages[state.messages.length - 1];
    const canAgreeToPeer = Boolean(lastMsg && lastMsg.from === peerRole);
    let effectiveStance = stance;
    let note = null;
    if (effectiveStance === 'agreed' && !canAgreeToPeer) {
      effectiveStance = lastMsg ? 'critique' : 'propose';
      note = lastMsg
        ? `Downgraded stance from "agreed" to "critique" because ${peerRole} has not replied to your latest message (#${lastMsg.seq}) yet.`
        : 'Downgraded stance from "agreed" to "propose" because peer has not reviewed or sent any message in this session yet.';
    }

    const nextSeq = state.seq + 1;
    state.seq = nextSeq;
    state.messages.push({
      seq: nextSeq,
      from: selfRole,
      to: peerRole,
      stance: effectiveStance,
      content: message,
      timestamp: nowIso,
    });
    state.cursors[selfRole] = nextSeq;
    state.updatedMs = nowMs;
    state.updatedAt = nowIso;

    if (effectiveStance === 'agreed') {
      state.status = 'converged';
      setPresence(state, selfRole, 'idle', `Agreed at #${nextSeq}`, nowMs);
      return {
        payload: buildConvergedPayload(
          state,
          selfRole,
          peerRole,
          nextSeq,
          `CONSENSUS REACHED: You (${selfRole}) agreed with ${peerRole}'s latest proposal (#${nextSeq}). ` +
            `${peerRole} has been notified and unlocked. Present the final agreed plan to the user. ${STANDBY_LISTENER_HINT}`
        ),
      };
    }

    state.status = 'active';
    const waitToken = waitForReply ? nextWaitToken() : null;
    setPresence(
      state,
      selfRole,
      waitForReply ? 'waiting' : 'idle',
      waitForReply ? waitingDetail(peerRole, nextSeq) : `Sent #${nextSeq} (${effectiveStance})`,
      nowMs,
      false,
      waitToken,
      false
    );

    if (!waitForReply) {
      return {
        payload: {
          status: 'sent',
          room: state.room,
          session_id: state.sessionId,
          self: selfRole,
          peer: peerRole,
          sent_seq: nextSeq,
          stance: effectiveStance,
          note: note || undefined,
          instruction: `Message #${nextSeq} queued for ${peerRole} without blocking. Call peer_wait() when you are ready to listen for their reply.`,
        },
      };
    }

    return {
      baselineSessionId: state.sessionId,
      sentSeq: nextSeq,
      hadActiveSession: true,
      lastHeartbeatMs: nowMs,
      peerPresence: state.presence[peerRole],
      note,
      waitToken,
      standby: false,
    };
  });

  if (sendOutcome.payload) {
    return sendOutcome.payload;
  }

  return waitForPeerTurn({
    room,
    selfRole,
    peerRole,
    timeoutSec,
    requestId,
    ...sendOutcome,
  });
}

async function handlePeerWait(args = {}, requestId) {
  const { selfRole, peerRole, room } = resolveContext(args);
  const timeoutSec = normalizeTimeoutSec(args.timeout_sec);
  const standby = Boolean(args.standby);

  cancelActiveWait(room, selfRole);

  const initialCheck = mutateRoomSync(room, (state) => {
    const unreadPayload = consumeUnreadPeerMessages(state, selfRole, 'message_received');
    if (unreadPayload) {
      return { payload: unreadPayload };
    }

    const nowMs = Date.now();
    const lastMsg = state.messages[state.messages.length - 1];
    if (state.status === 'active' && lastMsg && lastMsg.from === peerRole) {
      const wasWaiting = state.presence[selfRole]?.state === 'waiting';
      if (wasWaiting) {
        setPresence(state, selfRole, 'working', `Working on reply to #${lastMsg.seq}`, nowMs);
      }
      return {
        skipWrite: !wasWaiting,
        payload: {
          status: 'your_turn_to_reply',
          room: state.room,
          session_id: state.sessionId,
          self: selfRole,
          peer: peerRole,
          last_peer_seq: lastMsg.seq,
          instruction:
            `DO NOT WAIT: You (${selfRole}) already received ${peerRole}'s latest message (#${lastMsg.seq}) and it is YOUR turn to reply. ` +
            `Do not call peer_wait while running subagents, background tasks, or code reviews. ` +
            `Finish your analysis or wait for your subagents to complete first, then send your response with peer_exchange().`,
        },
      };
    }

    const sentSeq =
      state.status === 'active' && lastMsg && lastMsg.from === selfRole ? lastMsg.seq : null;
    const mutualGuard = checkMutualWaitGuard(state, selfRole, peerRole, sentSeq, nowMs, standby);
    if (mutualGuard) {
      return mutualGuard;
    }

    const desiredDetail = waitingDetail(peerRole, sentSeq);

    const prevPres = state.presence[selfRole];
    const alreadyWaitingFresh = isFreshWaitingPresence(
      prevPres,
      desiredDetail,
      nowMs,
      HEARTBEAT_UPDATE_MS,
      standby
    );
    const waitToken =
      alreadyWaitingFresh && prevPres?.waitToken ? prevPres.waitToken : nextWaitToken();

    if (!alreadyWaitingFresh) {
      setPresence(
        state,
        selfRole,
        'waiting',
        desiredDetail,
        nowMs,
        prevPres?.state === 'waiting',
        waitToken,
        standby
      );
    }
    return {
      skipWrite: alreadyWaitingFresh,
      baselineSessionId: state.sessionId,
      sentSeq,
      hadActiveSession: state.status === 'active',
      lastHeartbeatMs: alreadyWaitingFresh ? prevPres.updatedMs : nowMs,
      peerPresence: state.presence[peerRole],
      waitToken,
      standby,
    };
  });

  if (initialCheck.payload) {
    return initialCheck.payload;
  }

  return waitForPeerTurn({
    room,
    selfRole,
    peerRole,
    timeoutSec,
    requestId,
    ...initialCheck,
  });
}

function handlePeerStatus(args = {}) {
  const { selfRole, peerRole, room } = resolveContext(args);
  const action = String(args.action || 'status').trim().toLowerCase();

  if (action === 'reset') {
    return mutateRoomSync(room, (state) => ({
      payload: {
        status: 'reset_complete',
        room,
        archived_previous_to: resetRoomStateInPlace(state, room),
        new_session_id: state.sessionId,
        instruction: 'Room has been archived and reset. Ready for a new discussion.',
      },
    })).payload;
  }

  const state = readRoomStateRawSync(getRoomPaths(room).stateFile, room);
  return {
    status: 'ok',
    room: state.room,
    session_id: state.sessionId,
    topic: state.topic || '(untitled)',
    room_status: state.status,
    self: selfRole,
    peer: peerRole,
    seq: state.seq,
    unread_for_self: getUnreadPeerMessages(state, selfRole).length,
    cursors: state.cursors,
    presence: {
      antigravity: formatPresenceSummary(state.presence.antigravity),
      claude: formatPresenceSummary(state.presence.claude),
    },
    messages: state.messages,
  };
}

// `room` and `agent` are intentionally omitted from MCP_TOOLS schemas so each LLM uses its configured server identity on the default room.
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
      '5. If peer_exchange returns status="waiting_heartbeat" (because the peer is running long measurements), immediately call peer_wait() to keep waiting.\n' +
      '6. In your main agent turn, NEVER call peer_exchange(wait_for_reply=true) or foreground peer_wait() while you have a work subagent or background task running, because blocking in a foreground bridge tool prevents your own subagents from waking you up.\n' +
      '7. Standby Background Listener: Whenever a session reaches status="converged" (or between sessions when you want to stay connected on the bridge while keeping your main chat turn free to talk with the user), spawn a dedicated background listener subagent that calls peer_wait(standby=true) in a loop and wakes you up as soon as a peer message arrives.',
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
      },
      required: ['message', 'stance'],
    },
  },
  {
    name: 'peer_wait',
    description:
      'Listen and wait for the next message from the peer agent (claude or antigravity).\n\n' +
      'When to use:\n' +
      '1. Foreground wait: When the user tells you to listen on the bridge / wait for the other agent to send a plan, or immediately after peer_exchange / peer_wait returns status="waiting_heartbeat".\n' +
      '2. Standby background listener (standby=true): Spawn a dedicated background subagent (e.g. a subagent with MCP access in Antigravity or Agent with run_in_background=true in Claude Code) that calls peer_wait(standby=true, timeout_sec=120) in a loop on waiting_heartbeat and returns immediately on message_received or converged. This keeps your main chat turn free to talk with the user while automatically waking you up when the peer speaks.\n\n' +
      'When NOT to use:\n' +
      '- In your main agent turn, NEVER call foreground peer_wait while you have a work subagent or background task running.\n' +
      '- NEVER call peer_wait when you already received the peer\'s latest message and it is YOUR turn to reply (finish your work/subagents first, then call peer_exchange).\n' +
      '- NEVER call peer_wait if peer_wait returns status="your_turn_to_reply" or status="mutual_wait_prevented".\n\n' +
      'When a message arrives: summarize the peer\'s points for the user in your chat window, perform any needed checks/measurements, and reply with peer_exchange (unless status="converged").',
    inputSchema: {
      type: 'object',
      properties: {
        timeout_sec: {
          type: 'number',
          description:
            'Seconds to wait before returning a waiting_heartbeat reminder (default: 45, max: 120).',
        },
        standby: {
          type: 'boolean',
          description:
            'Set to true when called from a background listener subagent so both agents can stay on standby between sessions without triggering mutual_wait_prevented.',
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
  if (!process.stdout.writable) return;
  const json = JSON.stringify(msgObj);
  const out = useContentLengthFraming
    ? `Content-Length: ${Buffer.byteLength(json, 'utf-8')}\r\n\r\n${json}`
    : `${json}\n`;
  try {
    process.stdout.write(out);
  } catch {}
}

function sendResult(id, result) {
  sendJsonRpc({ jsonrpc: '2.0', id, result });
}

async function handleJsonRpcMessage(msg) {
  if (!msg || typeof msg !== 'object') return;

  const { id, method, params } = msg;
  const isNotification = !Object.prototype.hasOwnProperty.call(msg, 'id') || id === undefined;

  try {
    if (method === 'initialize') {
      const clientName = params?.clientInfo?.name || '';
      if (!detectedSelfRole && clientName) {
        detectedSelfRole = normalizeAgentRole(clientName);
      }
      if (!isNotification) {
        sendResult(id, {
          protocolVersion: params?.protocolVersion || '2024-11-05',
          capabilities: { tools: {} },
          serverInfo: { name: SERVER_NAME, version: SERVER_VERSION },
        });
      }
      return;
    }

    if (method === 'notifications/initialized' || method === 'initialized') return;

    if (method === 'notifications/cancelled' || method === '$/cancelRequest') {
      cancelWaitByRequestId(params?.requestId ?? params?.id);
      return;
    }

    if (method === 'ping') {
      if (!isNotification) sendResult(id, {});
      return;
    }

    if (method === 'tools/list') {
      if (!isNotification) sendResult(id, { tools: MCP_TOOLS });
      return;
    }

    if (method === 'resources/list') {
      if (!isNotification) sendResult(id, { resources: [] });
      return;
    }

    if (method === 'prompts/list') {
      if (!isNotification) sendResult(id, { prompts: [] });
      return;
    }

    if (method === 'tools/call') {
      const toolName = params?.name;
      const rawArgs = params?.arguments;
      if (rawArgs != null && (typeof rawArgs !== 'object' || Array.isArray(rawArgs))) {
        throw new Error('Parameter "arguments" must be a JSON object.');
      }
      const toolArgs = rawArgs || {};
      let resultPayload;

      if (toolName === 'peer_exchange') {
        resultPayload = await handlePeerExchange(toolArgs, id);
      } else if (toolName === 'peer_wait') {
        resultPayload = await handlePeerWait(toolArgs, id);
      } else if (toolName === 'peer_status') {
        resultPayload = handlePeerStatus(toolArgs);
      } else {
        throw new Error(`Unknown tool: ${toolName}`);
      }

      if (
        isNotification ||
        (resultPayload?.status === 'cancelled' && resultPayload?.reason === 'rpc_cancelled')
      ) {
        return;
      }

      sendResult(id, {
        content: [{ type: 'text', text: JSON.stringify(resultPayload, null, 2) }],
      });
      return;
    }

    if (!isNotification) {
      sendJsonRpc({
        jsonrpc: '2.0',
        id,
        error: { code: -32601, message: `Method not found: ${method}` },
      });
    }
  } catch (err) {
    if (!isNotification) {
      sendResult(id, {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify(
              { status: 'error', error: err instanceof Error ? err.message : String(err) },
              null,
              2
            ),
          },
        ],
      });
    }
  }
}

const EMPTY_BUF = Buffer.alloc(0);
const MAX_HEADER_BYTES = 4096;
const MAX_MESSAGE_BYTES = 16 * 1024 * 1024;

function startMcpStdioServer() {
  let buffer = EMPTY_BUF;
  let exiting = false;
  let stdinClosed = false;
  let inFlight = 0;

  const cleanupAndExit = () => {
    if (exiting) return;
    exiting = true;
    if (detectedSelfRole && hasActivePresence) {
      try {
        mutateRoomSync(cliOpts.room, (state) => {
          if (state.presence[detectedSelfRole]?.state !== 'idle') {
            setPresence(state, detectedSelfRole, 'idle', 'Disconnected', Date.now());
            return {};
          }
          return { skipWrite: true };
        });
      } catch {}
    }
    process.exit(0);
  };

  const onStdinEnd = () => {
    stdinClosed = true;
    if (inFlight === 0) cleanupAndExit();
  };

  process.stdin.on('data', (chunk) => {
    buffer = buffer.length === 0 ? chunk : Buffer.concat([buffer, chunk]);
    let offset = 0;

    // Auto-detect LSP Content-Length framing vs newline-delimited JSON-RPC so both MCP stdio styles work.
    while (offset < buffer.length) {
      while (
        offset < buffer.length &&
        (buffer[offset] === 0x0d ||
          buffer[offset] === 0x0a ||
          buffer[offset] === 0x20 ||
          buffer[offset] === 0x09)
      ) {
        offset++;
      }
      if (offset >= buffer.length) break;

      let rawJson;
      if (buffer[offset] !== 0x7b /* '{' */) {
        const scanEnd = Math.min(buffer.length, offset + MAX_HEADER_BYTES);
        const headerSlice = buffer.subarray(offset, scanEnd);
        const crlfRel = headerSlice.indexOf('\r\n\r\n');
        const lfSearchEnd = crlfRel !== -1 ? crlfRel : headerSlice.length;
        const lfRel = headerSlice.subarray(0, lfSearchEnd).indexOf('\n\n');
        const hasLf = lfRel !== -1;
        const relEnd = hasLf ? lfRel : crlfRel;

        if (relEnd === -1) {
          if (buffer.length - offset >= MAX_HEADER_BYTES) {
            buffer = EMPTY_BUF;
            offset = 0;
          }
          break;
        }

        const headerEnd = offset + relEnd;
        const sepLen = hasLf ? 2 : 4;
        const bodyStart = headerEnd + sepLen;
        const match = buffer
          .toString('utf-8', offset, headerEnd)
          .match(/Content-Length:\s*(\d+)/i);
        if (!match) {
          offset = bodyStart;
          continue;
        }
        const contentLen = parseInt(match[1], 10);
        if (contentLen > MAX_MESSAGE_BYTES) {
          offset = bodyStart;
          continue;
        }
        useContentLengthFraming = true;
        if (buffer.length < bodyStart + contentLen) break;

        rawJson = buffer.toString('utf-8', bodyStart, bodyStart + contentLen);
        offset = bodyStart + contentLen;
      } else {
        const nlIdx = buffer.indexOf(0x0a, offset);
        if (nlIdx === -1) {
          if (buffer.length - offset > MAX_MESSAGE_BYTES) {
            buffer = EMPTY_BUF;
            offset = 0;
          }
          break;
        }
        rawJson = buffer.toString('utf-8', offset, nlIdx);
        offset = nlIdx + 1;
      }

      let parsedMsg;
      try {
        parsedMsg = JSON.parse(rawJson);
      } catch {
        continue;
      }
      inFlight++;
      handleJsonRpcMessage(parsedMsg)
        .catch(() => {})
        .finally(() => {
          if (--inFlight === 0 && stdinClosed) cleanupAndExit();
        });
    }

    if (offset >= buffer.length) {
      buffer = EMPTY_BUF;
    } else if (offset > 0) {
      buffer = Buffer.from(buffer.subarray(offset));
    }
  });

  process.stdin.on('end', onStdinEnd);
  process.stdin.on('close', onStdinEnd);
  process.stdin.on('error', cleanupAndExit);
  process.stdout.on('error', cleanupAndExit);
  process.on('SIGINT', cleanupAndExit);
  process.on('SIGTERM', cleanupAndExit);
  process.on('SIGHUP', cleanupAndExit);
}

async function runSelfTest() {
  const testRoom = `selftest_${process.pid}`;
  const { stateFile, lockFile } = getRoomPaths(testRoom);
  const check = (cond, label, data) => {
    if (!cond) throw new Error(`${label} failed${data !== undefined ? `: ${JSON.stringify(data)}` : ''}`);
  };
  const send = (agent, stance, message, extra = {}) =>
    handlePeerExchange({ room: testRoom, agent, stance, message, timeout_sec: 10, ...extra });

  try {
    // 1. Reset room + verify first-turn "agreed" downgrade to "propose" (blocking + non-blocking) + archive on reset
    check(handlePeerStatus({ room: testRoom, action: 'reset' }).status === 'reset_complete', 'Reset');
    const down = await send('antigravity', 'agreed', 'Premature agreed', { wait_for_reply: false });
    check(down.status === 'sent' && down.stance === 'propose' && down.note, 'Downgrade', down);
    const arch = handlePeerStatus({ room: testRoom, action: 'reset' });
    check(arch.archived_previous_to && fs.existsSync(arch.archived_previous_to), 'Archive', arch);

    // 2. Multi-round concurrent debate (#1 propose -> #2 critique -> #3 critique -> #4 agreed)
    const claudeWait1 = handlePeerWait({ room: testRoom, agent: 'claude', timeout_sec: 10 });
    const agyEx1 = send('antigravity', 'agreed', 'Plan v1: LRU + TTL 60s', { topic: 'Cache' });
    const cRecv1 = await claudeWait1;
    check(
      cRecv1.status === 'message_received' &&
        cRecv1.peer_message?.seq === 1 &&
        cRecv1.peer_message?.stance === 'propose' &&
        cRecv1.topic === 'Cache',
      'cRecv1',
      cRecv1
    );

    const claudeEx1 = send('claude', 'critique', 'Point 2 disagree: TTL 300s');
    const aRecv1 = await agyEx1;
    check(
      aRecv1.status === 'message_received' && aRecv1.peer_message?.seq === 2 && Boolean(aRecv1.note),
      'aRecv1',
      aRecv1
    );

    const agyEx2 = send('antigravity', 'critique', 'Plan v2: TTL 300s + hook');
    const cRecv2 = await claudeEx1;
    check(cRecv2.status === 'message_received' && cRecv2.peer_message?.seq === 3, 'cRecv2', cRecv2);

    const cAgree = await send('claude', 'agreed', '100% agreed with Plan v2');
    const aConv = await agyEx2;
    check(cAgree.status === 'converged' && aConv.status === 'converged', 'Converge', { cAgree, aConv });

    // Idempotent post-convergence "agreed" does not append duplicate message
    const dupAgree = await send('antigravity', 'agreed', 'Redundant agreed after convergence');
    check(dupAgree.status === 'converged' && dupAgree.sent_seq === 4, 'IdempotentAgree', dupAgree);

    // 3. Unread inbox guard (multi-message batch #5+#6 and converged/agreed #7) + unilateral agreed downgrade
    await send('claude', 'critique', 'Extra check on memory cap (#5)', { wait_for_reply: false });
    const selfAgreeDown = await send('claude', 'agreed', 'Unilateral self-agree (#6)', {
      wait_for_reply: false,
    });
    check(
      selfAgreeDown.status === 'sent' && selfAgreeDown.stance === 'critique' && Boolean(selfAgreeDown.note),
      'UnilateralAgreeDowngrade',
      selfAgreeDown
    );
    const guard1 = await send('antigravity', 'critique', 'Blind send', { wait_for_reply: false });
    check(
      guard1.status === 'unread_inbox_first' &&
        guard1.peer_message?.seq === 6 &&
        guard1.all_unread_delivered?.length === 2,
      'Guard1',
      guard1
    );

    const send7 = await send('antigravity', 'agreed', 'Agreed on memory cap (#7)', { wait_for_reply: false });
    check(send7.status === 'converged' && send7.sent_seq === 7, 'Send7', send7);

    const guard2 = await send('claude', 'critique', 'Blind send before #7', { wait_for_reply: false });
    check(
      guard2.status === 'unread_inbox_first' &&
        guard2.peer_message?.seq === 7 &&
        handlePeerStatus({ room: testRoom, agent: 'claude' }).presence.claude.startsWith('idle'),
      'Guard2',
      guard2
    );

    // 4. Stale session (STALE_SESSION_MS) must NOT wipe unread messages when either sender or receiver calls handlePeerWait
    const passiveWait = handlePeerWait({ room: testRoom, agent: 'claude', timeout_sec: 10 });
    await send('antigravity', 'propose', 'New topic plan (#1)', {
      new_session: true,
      wait_for_reply: false,
    });
    const passiveRecv = await passiveWait;
    check(
      passiveRecv.status === 'message_received' &&
        passiveRecv.peer_message?.seq === 1 &&
        passiveRecv.note === 'Peer started a new session.',
      'PassiveWaitNewSession',
      passiveRecv
    );

    await send('antigravity', 'propose', 'Stale topic plan (#1)', {
      new_session: true,
      wait_for_reply: false,
    });
    mutateRoomSync(testRoom, (s) => {
      s.updatedMs -= STALE_SESSION_MS + 5000;
      return {};
    });
    const senderStaleWait = await handlePeerWait({ room: testRoom, agent: 'antigravity', timeout_sec: 1 });
    check(senderStaleWait.status === 'waiting_heartbeat' && senderStaleWait.sent_seq === 1, 'SenderStaleWait', senderStaleWait);
    const staleRecv = await handlePeerWait({ room: testRoom, agent: 'claude', timeout_sec: 2 });
    check(staleRecv.status === 'message_received' && staleRecv.peer_message?.seq === 1, 'StaleUnread', staleRecv);
    const wrongTurnWait = await handlePeerWait({ room: testRoom, agent: 'claude', timeout_sec: 10 });
    check(
      wrongTurnWait.status === 'your_turn_to_reply' && wrongTurnWait.last_peer_seq === 1,
      'WrongTurnGuard',
      wrongTurnWait
    );

    // 5. Room reset while waiting in active session unlocks waiter with session_reset
    await send('claude', 'critique', 'Waiting for reply (#2)', { wait_for_reply: false });
    const waitResetP = handlePeerWait({ room: testRoom, agent: 'claude', timeout_sec: 10 });
    handlePeerStatus({ room: testRoom, action: 'reset' });
    const resetWake = await waitResetP;
    check(resetWake.status === 'session_reset', 'SessionResetWake', resetWake);

    // 6. Cancellation safety (rpc_cancelled + superseded) + 1s heartbeat timeout + zero-write polling verification
    const supersededP = handlePeerWait({ room: testRoom, agent: 'claude', timeout_sec: 10 });
    const cancelP = handlePeerWait({ room: testRoom, agent: 'claude', timeout_sec: 10 }, 'req-99');
    const supRes = await supersededP;
    check(supRes.status === 'cancelled' && supRes.reason === 'superseded', 'Superseded', supRes);
    cancelWaitByRequestId('req-99');
    const cancelRes = await cancelP;
    check(
      cancelRes.status === 'cancelled' &&
        cancelRes.reason === 'rpc_cancelled' &&
        handlePeerStatus({ room: testRoom, agent: 'claude' }).presence.claude.startsWith('idle'),
      'Cancel',
      cancelRes
    );

    // First 1s wait transitions claude from idle -> waiting; second 1s wait must do 0 writes
    const hb1 = await handlePeerWait({ room: testRoom, agent: 'claude', timeout_sec: 1 });
    check(hb1.status === 'waiting_heartbeat' && hb1.waited_sec === 1, 'Heartbeat1', hb1);
    const mtimeBefore = fs.statSync(stateFile).mtimeMs;
    const hb2 = await handlePeerWait({ room: testRoom, agent: 'claude', timeout_sec: 1 });
    const mutualWait = await handlePeerWait({ room: testRoom, agent: 'antigravity', timeout_sec: 10 });
    const mtimeAfter = fs.statSync(stateFile).mtimeMs;
    check(
      hb2.status === 'waiting_heartbeat' &&
        mutualWait.status === 'mutual_wait_prevented' &&
        mtimeAfter === mtimeBefore,
      'ZeroWriteAndMutualWaitGuard',
      { mtimeBefore, mtimeAfter, mutualWait }
    );

    // 6b. Dual standby background listeners + cross-process waitToken supersede without clobbering presence
    const claudeStandby = handlePeerWait({
      room: testRoom,
      agent: 'claude',
      timeout_sec: 10,
      standby: true,
    });
    const agyStandby = handlePeerWait({
      room: testRoom,
      agent: 'antigravity',
      timeout_sec: 10,
      standby: true,
    });
    const newCallerToken = nextWaitToken();
    mutateRoomSync(testRoom, (s) => {
      resetRoomStateInPlace(s, testRoom);
      const nowMs = Date.now();
      s.status = 'active';
      s.seq = 1;
      s.messages.push({
        seq: 1,
        from: 'claude',
        to: 'antigravity',
        stance: 'propose',
        content: 'Cross-process topic (#1)',
        timestamp: new Date(nowMs).toISOString(),
      });
      s.cursors.claude = 1;
      setPresence(
        s,
        'claude',
        'waiting',
        waitingDetail('antigravity', 1),
        nowMs,
        false,
        newCallerToken,
        false
      );
      return {};
    });
    const [claudeSup, agyWoken] = await Promise.all([claudeStandby, agyStandby]);
    const postStandbyState = readRoomStateRawSync(stateFile, testRoom);
    check(
      claudeSup.status === 'cancelled' &&
        claudeSup.reason === 'superseded' &&
        agyWoken.status === 'message_received' &&
        agyWoken.peer_message?.seq === 1 &&
        postStandbyState.presence.claude?.waitToken === newCallerToken,
      'StandbyAndCrossProcessSupersede',
      { claudeSup, agyWoken, claudePres: postStandbyState.presence.claude }
    );

    // 7. Corrupt JSON backup (moved once, not duplicated on subsequent status reads) & recovery
    fs.writeFileSync(stateFile, '{corrupt json', 'utf-8');
    const rec = handlePeerStatus({ room: testRoom, action: 'status' });
    const corruptBackups = fs
      .readdirSync(ARCHIVE_DIR)
      .filter((f) => f.startsWith(`corrupt-${testRoom}-`));
    check(
      rec.status === 'ok' &&
        rec.seq === 0 &&
        corruptBackups.length === 1 &&
        !fs.existsSync(stateFile),
      'CorruptRecovery',
      { rec, corruptBackups }
    );

    console.log(
      JSON.stringify(
        {
          selftest: 'PASSED',
          convergence_verified: true,
          idempotent_agreed_verified: true,
          unread_inbox_guard_verified: true,
          stale_unread_preservation_verified: true,
          session_reset_wake_verified: true,
          cancel_safety_verified: true,
          no_op_wait_zero_writes_verified: true,
          wrong_turn_and_mutual_wait_guards_verified: true,
          standby_and_cross_process_supersede_verified: true,
          corrupt_recovery_verified: true,
        },
        null,
        2
      )
    );
  } finally {
    try {
      if (fs.existsSync(stateFile)) fs.unlinkSync(stateFile);
      if (fs.existsSync(lockFile)) fs.unlinkSync(lockFile);
      if (fs.existsSync(ARCHIVE_DIR)) {
        for (const f of fs.readdirSync(ARCHIVE_DIR)) {
          if (f.startsWith(`${testRoom}-`) || f.startsWith(`corrupt-${testRoom}-`)) {
            fs.unlinkSync(path.join(ARCHIVE_DIR, f));
          }
        }
      }
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
        '  node server.cjs --agent antigravity [--room default]',
        '  node server.cjs --agent claude [--room default]',
        '',
        'CLI Diagnostics Mode:',
        '  node server.cjs --status [--room default]',
        '  node server.cjs --reset [--room default]',
        '  node server.cjs --selftest',
        '  node server.cjs --help',
        '',
        'Environment Variables:',
        '  AGENT_BRIDGE_DATA_DIR   Override room state directory (default: ./data)',
        '  AGENT_BRIDGE_ROLE       Default agent identity (antigravity | claude)',
      ].join('\n')
    );
    return;
  }

  if (cliOpts.cliMode === 'selftest') {
    await runSelfTest();
    return;
  }

  if (cliOpts.cliMode === 'status' || cliOpts.cliMode === 'reset') {
    console.log(
      JSON.stringify(handlePeerStatus({ room: cliOpts.room, action: cliOpts.cliMode }), null, 2)
    );
  }
}

module.exports = {
  readJsonSafe,
  writeJsonAtomic,
};

if (require.main === module) {
  if (cliOpts.cliMode) {
    runCliMode().catch((err) => {
      console.error(err instanceof Error ? err.message : String(err));
      process.exit(1);
    });
  } else {
    startMcpStdioServer();
  }
}
