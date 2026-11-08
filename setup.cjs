#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const REPO_ROOT = __dirname;
const SERVER_SCRIPT = path.join(REPO_ROOT, 'server.cjs');

const GEMINI_MCP_CONFIG = process.env.ANTIGRAVITY_CONFIG_DIR
  ? path.join(path.resolve(process.env.ANTIGRAVITY_CONFIG_DIR), 'mcp_config.json')
  : path.join(os.homedir(), '.gemini', 'config', 'mcp_config.json');

const CLAUDE_CONFIG = path.join(os.homedir(), '.claude.json');

function resolveNodeExecutable() {
  const execPath = process.execPath;
  if (execPath && fs.existsSync(execPath)) {
    return execPath;
  }
  return 'node';
}

function readJsonSafe(filePath) {
  if (!fs.existsSync(filePath)) return {};
  const raw = fs.readFileSync(filePath, 'utf-8').replace(/^\uFEFF/, '');
  return JSON.parse(raw);
}

function writeJsonAtomic(filePath, obj) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  const tmpFile = `${filePath}.tmp.${process.pid}`;
  fs.writeFileSync(tmpFile, JSON.stringify(obj, null, 2) + '\n', 'utf-8');
  fs.renameSync(tmpFile, filePath);
}

function checkStatus() {
  const nodeBin = resolveNodeExecutable();
  let antigravityConfigured = false;
  let claudeConfigured = false;

  try {
    const geminiObj = readJsonSafe(GEMINI_MCP_CONFIG);
    const entry = geminiObj?.mcpServers?.['agent-bridge'];
    if (
      entry &&
      Array.isArray(entry.args) &&
      entry.args.includes(SERVER_SCRIPT) &&
      entry.args.includes('antigravity')
    ) {
      antigravityConfigured = true;
    }
  } catch {}

  try {
    const claudeObj = readJsonSafe(CLAUDE_CONFIG);
    const entry = claudeObj?.mcpServers?.['agent-bridge'];
    if (
      entry &&
      Array.isArray(entry.args) &&
      entry.args.includes(SERVER_SCRIPT) &&
      entry.args.includes('claude')
    ) {
      claudeConfigured = true;
    }
  } catch {}

  return {
    nodeExecutable: nodeBin,
    serverScript: SERVER_SCRIPT,
    serverExists: fs.existsSync(SERVER_SCRIPT),
    geminiMcpConfig: GEMINI_MCP_CONFIG,
    antigravityConfigured,
    claudeConfig: CLAUDE_CONFIG,
    claudeConfigured,
  };
}

function installConfigs() {
  const nodeBin = resolveNodeExecutable();

  // 1. Update Antigravity ~/.gemini/config/mcp_config.json
  const geminiObj = readJsonSafe(GEMINI_MCP_CONFIG);
  if (!geminiObj.mcpServers || typeof geminiObj.mcpServers !== 'object') {
    geminiObj.mcpServers = {};
  }
  geminiObj.mcpServers['agent-bridge'] = {
    command: nodeBin,
    args: [SERVER_SCRIPT, '--agent', 'antigravity'],
  };
  writeJsonAtomic(GEMINI_MCP_CONFIG, geminiObj);

  // 2. Update Claude Code ~/.claude.json
  const claudeObj = readJsonSafe(CLAUDE_CONFIG);
  if (!claudeObj.mcpServers || typeof claudeObj.mcpServers !== 'object') {
    claudeObj.mcpServers = {};
  }
  claudeObj.mcpServers['agent-bridge'] = {
    type: 'stdio',
    command: nodeBin,
    args: [SERVER_SCRIPT, '--agent', 'claude'],
    env: {},
  };
  writeJsonAtomic(CLAUDE_CONFIG, claudeObj);

  return checkStatus();
}

function uninstallConfigs() {
  if (fs.existsSync(GEMINI_MCP_CONFIG)) {
    const geminiObj = readJsonSafe(GEMINI_MCP_CONFIG);
    if (geminiObj?.mcpServers?.['agent-bridge']) {
      delete geminiObj.mcpServers['agent-bridge'];
      writeJsonAtomic(GEMINI_MCP_CONFIG, geminiObj);
    }
  }
  if (fs.existsSync(CLAUDE_CONFIG)) {
    const claudeObj = readJsonSafe(CLAUDE_CONFIG);
    if (claudeObj?.mcpServers?.['agent-bridge']) {
      delete claudeObj.mcpServers['agent-bridge'];
      writeJsonAtomic(CLAUDE_CONFIG, claudeObj);
    }
  }
  return checkStatus();
}

const args = process.argv.slice(2);
if (args.includes('--check')) {
  console.log(JSON.stringify(checkStatus(), null, 2));
} else if (args.includes('--uninstall')) {
  console.log(JSON.stringify(uninstallConfigs(), null, 2));
} else {
  console.log(JSON.stringify(installConfigs(), null, 2));
}
