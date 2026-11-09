#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const SERVER_SCRIPT = path.join(__dirname, 'server.cjs');
const { readJsonSafe, writeJsonAtomic } = require('./server.cjs');

const GEMINI_MCP_CONFIG = process.env.ANTIGRAVITY_CONFIG_DIR
  ? path.join(path.resolve(process.env.ANTIGRAVITY_CONFIG_DIR), 'mcp_config.json')
  : path.join(os.homedir(), '.gemini', 'config', 'mcp_config.json');

const CLAUDE_CONFIG = process.env.CLAUDE_CONFIG_FILE
  ? path.resolve(process.env.CLAUDE_CONFIG_FILE)
  : path.join(os.homedir(), '.claude.json');

const RESOLVED_NODE_BIN =
  process.execPath &&
  /^node(\.exe)?$/i.test(path.basename(process.execPath)) &&
  fs.existsSync(process.execPath)
    ? process.execPath
    : 'node';

function normalizePathForCompare(p) {
  const resolved = path.resolve(String(p || ''));
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

const EXPECTED_SERVER_SCRIPT = normalizePathForCompare(SERVER_SCRIPT);

function hasBridgeEntry(filePath, agentName) {
  try {
    const entry = readJsonSafe(filePath).mcpServers?.['agent-bridge'];
    if (!entry || !Array.isArray(entry.args)) return false;
    const cmdOk =
      typeof entry.command === 'string' &&
      entry.command.length > 0 &&
      (entry.command === 'node' || fs.existsSync(entry.command));
    if (!cmdOk) return false;
    const hasScript = entry.args.some(
      (arg) =>
        typeof arg === 'string' &&
        path.isAbsolute(arg) &&
        normalizePathForCompare(arg) === EXPECTED_SERVER_SCRIPT
    );
    let resolvedAgent = null;
    for (let i = 0; i < entry.args.length; i++) {
      const arg = entry.args[i];
      if (arg === '--agent' && typeof entry.args[i + 1] === 'string') {
        resolvedAgent = entry.args[++i];
      } else if (typeof arg === 'string' && arg.startsWith('--agent=')) {
        resolvedAgent = arg.slice('--agent='.length);
      }
    }
    return hasScript && resolvedAgent === agentName;
  } catch {
    return false;
  }
}

function upsertBridgeEntry(filePath, entryValue) {
  const configObj = readJsonSafe(filePath);
  if (
    !configObj.mcpServers ||
    typeof configObj.mcpServers !== 'object' ||
    Array.isArray(configObj.mcpServers)
  ) {
    configObj.mcpServers = {};
  }
  if (JSON.stringify(configObj.mcpServers['agent-bridge']) !== JSON.stringify(entryValue)) {
    configObj.mcpServers['agent-bridge'] = entryValue;
    writeJsonAtomic(filePath, configObj);
  }
}

function removeBridgeEntry(filePath) {
  const configObj = readJsonSafe(filePath);
  if (configObj.mcpServers?.['agent-bridge']) {
    delete configObj.mcpServers['agent-bridge'];
    writeJsonAtomic(filePath, configObj);
  }
}

function checkStatus() {
  return {
    nodeExecutable: RESOLVED_NODE_BIN,
    serverScript: SERVER_SCRIPT,
    serverExists: fs.existsSync(SERVER_SCRIPT),
    geminiMcpConfig: GEMINI_MCP_CONFIG,
    antigravityConfigured: hasBridgeEntry(GEMINI_MCP_CONFIG, 'antigravity'),
    claudeConfig: CLAUDE_CONFIG,
    claudeConfigured: hasBridgeEntry(CLAUDE_CONFIG, 'claude'),
  };
}

function installConfigs() {
  upsertBridgeEntry(GEMINI_MCP_CONFIG, {
    command: RESOLVED_NODE_BIN,
    args: [SERVER_SCRIPT, '--agent', 'antigravity'],
  });

  upsertBridgeEntry(CLAUDE_CONFIG, {
    type: 'stdio',
    command: RESOLVED_NODE_BIN,
    args: [SERVER_SCRIPT, '--agent', 'claude'],
    env: {},
  });

  return checkStatus();
}

function uninstallConfigs() {
  removeBridgeEntry(GEMINI_MCP_CONFIG);
  removeBridgeEntry(CLAUDE_CONFIG);
  return checkStatus();
}

const args = process.argv.slice(2);
const mode = args.length === 0 ? 'install' : args.length === 1 ? args[0] : null;
if (mode === '--help' || mode === '-h') {
  console.log(
    [
      'Usage: node setup.cjs [options]',
      '',
      'Options:',
      '  (no args)     Install agent-bridge into Antigravity and Claude Code MCP configs',
      '  --check       Verify installation status (exits 1 if not fully configured)',
      '  --uninstall   Remove agent-bridge from Antigravity and Claude Code MCP configs',
      '  --help, -h    Show this help message',
      '',
      'Environment Variables:',
      '  ANTIGRAVITY_CONFIG_DIR   Override ~/.gemini/config directory',
      '  CLAUDE_CONFIG_FILE       Override ~/.claude.json file path',
    ].join('\n')
  );
} else if (mode === '--check' || mode === 'install') {
  const status = mode === 'install' ? installConfigs() : checkStatus();
  console.log(JSON.stringify(status, null, 2));
  if (!status.serverExists || !status.antigravityConfigured || !status.claudeConfigured) {
    process.exitCode = 1;
  }
} else if (mode === '--uninstall') {
  console.log(JSON.stringify(uninstallConfigs(), null, 2));
} else {
  console.error(`Unknown argument(s): ${args.join(' ')}. Run "node setup.cjs --help" for usage.`);
  process.exitCode = 1;
}
