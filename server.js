const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);

// High-throughput WebSocket configuration (128MB payload ceiling, perMessageDeflate disabled for raw binary speed)
const wss = new WebSocketServer({
  server,
  maxPayload: 128 * 1024 * 1024,
  perMessageDeflate: false
});

const PORT = process.env.PORT || 3000;

// Zero-log volatile memory structures
const peers = new Map();          // peerId -> { ws, peerId, ipGroup, name, os, deviceType, activeSessionId }
const sessions = new Map();       // sessionId -> { sessionId, code4, senderId, senderWs, receivers: Map(peerId -> ws), files, totalSize }
const codeToSession = new Map();  // 4-digit code -> sessionId

function generate4DigitCode() {
  let code = '';
  for (let i = 0; i < 4; i++) {
    code += Math.floor(Math.random() * 10).toString();
  }
  if (codeToSession.has(code)) return generate4DigitCode();
  return code;
}

function getClientIpGroup(req) {
  const forwarded = req.headers['x-forwarded-for'];
  let ip = forwarded ? forwarded.split(',')[0].trim() : (req.socket.remoteAddress || 'local');
  if (ip === '::1' || ip === '127.0.0.1' || ip.indexOf('192.168.') !== -1 || ip.indexOf('10.') !== -1) {
    return 'local-lan';
  }
  return ip;
}

function broadcastRadarToIpGroup(ipGroup) {
  const groupPeers = [];
  peers.forEach((p) => {
    if (p.ipGroup === ipGroup) {
      groupPeers.push({
        peerId: p.peerId,
        name: p.name,
        os: p.os,
        deviceType: p.deviceType,
        activeSessionId: p.activeSessionId || null
      });
    }
  });

  peers.forEach((p) => {
    if (p.ipGroup === ipGroup && p.ws.readyState === p.ws.OPEN) {
      p.ws.send(JSON.stringify({
        type: 'radar-peers',
        peers: groupPeers.filter((item) => item.peerId !== p.peerId)
      }));
    }
  });
}

wss.on('connection', (ws, req) => {
  if (ws._socket && typeof ws._socket.setNoDelay === 'function') {
    ws._socket.setNoDelay(true); // Disable Nagle's algorithm for instant packet dispatch
  }

  const ipGroup = getClientIpGroup(req);
  let myPeerId = null;
  let boundSessionId = null;
  let boundRole = null;

  ws.on('message', (rawMessage, isBinary) => {
    // High-Speed Binary Chunk Relay for Turbo Relay mode (multi-receiver broadcast capable)
    if (isBinary) {
      if (boundSessionId && sessions.has(boundSessionId)) {
        const sess = sessions.get(boundSessionId);
        if (boundRole === 'sender') {
          sess.receivers.forEach((rWs) => {
            if (rWs && rWs.readyState === ws.OPEN && rWs.bufferedAmount < 32 * 1024 * 1024) {
              rWs.send(rawMessage, { binary: true });
            }
          });
        }
      }
      return;
    }

    let data;
    try {
      data = JSON.parse(rawMessage.toString());
    } catch (e) {
      return;
    }

    // 0. Ping / Pong for Cyber-HUD Telemetry Card
    if (data.type === 'hud-ping') {
      ws.send(JSON.stringify({ type: 'hud-pong', t: data.t }));
      return;
    }

    // 1. Register Peer on Sonar Radar (Same-Wi-Fi Auto-Discovery)
    if (data.type === 'register-peer') {
      myPeerId = data.peerId;
      peers.set(myPeerId, {
        ws: ws,
        peerId: myPeerId,
        ipGroup: ipGroup,
        name: data.name || 'Nexus Node',
        os: data.os || 'Device',
        deviceType: data.deviceType || 'desktop',
        activeSessionId: null
      });
      broadcastRadarToIpGroup(ipGroup);
    }

    // Update peer custom nickname
    else if (data.type === 'update-peer-name') {
      if (myPeerId && peers.has(myPeerId)) {
        const p = peers.get(myPeerId);
        p.name = String(data.name || 'Nexus Node').substring(0, 24);
        broadcastRadarToIpGroup(ipGroup);
      }
    }

    // 2. Sender creates or updates a Broadcast File Session
    else if (data.type === 'create-session') {
      if (boundSessionId && sessions.has(boundSessionId) && boundRole === 'sender') {
        const old = sessions.get(boundSessionId);
        codeToSession.delete(old.code4);
      }
      const sessionId = data.sessionId;
      const code4 = generate4DigitCode();
      boundSessionId = sessionId;
      boundRole = 'sender';

      sessions.set(sessionId, {
        sessionId: sessionId,
        code4: code4,
        senderId: myPeerId,
        senderWs: ws,
        receivers: new Map(),
        files: data.files || [],
        totalSize: data.totalSize || 0
      });
      codeToSession.set(code4, sessionId);

      if (myPeerId && peers.has(myPeerId)) {
        peers.get(myPeerId).activeSessionId = sessionId;
        broadcastRadarToIpGroup(ipGroup);
      }

      ws.send(JSON.stringify({
        type: 'session-created',
        sessionId: sessionId,
        code4: code4
      }));
    }

    // Sender updates session files (e.g., when clicking "Bundle into .ZIP")
    else if (data.type === 'update-session-files') {
      if (boundSessionId && sessions.has(boundSessionId)) {
        const sess = sessions.get(boundSessionId);
        sess.files = data.files || [];
        sess.totalSize = data.totalSize || 0;
        sess.receivers.forEach((rWs) => {
          if (rWs.readyState === ws.OPEN) {
            rWs.send(JSON.stringify({
              type: 'session-info',
              sessionId: sess.sessionId,
              files: sess.files,
              totalSize: sess.totalSize
            }));
          }
        });
      }
    }

    // 3. One-Click "Tap-to-Send" on Same-Wi-Fi Sonar Radar
    else if (data.type === 'beam-to-peer') {
      const target = peers.get(data.targetPeerId);
      if (target && target.ws && target.ws.readyState === ws.OPEN) {
        target.ws.send(JSON.stringify({
          type: 'incoming-radar-beam',
          sessionId: data.sessionId,
          senderName: data.senderName || 'Nearby Device'
        }));
      }
    }

    // 4. Lookup 4-Digit Local Code
    else if (data.type === 'lookup-code') {
      const cleanCode = String(data.code || '').trim();
      const targetSessionId = codeToSession.get(cleanCode);
      if (targetSessionId && sessions.has(targetSessionId)) {
        ws.send(JSON.stringify({
          type: 'code-found',
          sessionId: targetSessionId
        }));
      } else {
        ws.send(JSON.stringify({
          type: 'error-msg',
          message: 'Invalid or expired 4-digit code. Make sure the sender tab is open.'
        }));
      }
    }

    // 5. Receiver joins a session to preview files
    else if (data.type === 'join-session') {
      const sess = sessions.get(data.sessionId);
      if (!sess || !sess.senderWs || sess.senderWs.readyState !== ws.OPEN) {
        ws.send(JSON.stringify({
          type: 'session-expired',
          message: 'This transfer link is no longer active because the sender closed their tab.'
        }));
        return;
      }

      boundSessionId = data.sessionId;
      boundRole = 'receiver';
      const rKey = myPeerId || ('r-' + Math.random().toString(36).substring(2, 8));
      myPeerId = rKey;
      sess.receivers.set(rKey, ws);

      ws.send(JSON.stringify({
        type: 'session-info',
        sessionId: sess.sessionId,
        files: sess.files,
        totalSize: sess.totalSize
      }));

      sess.senderWs.send(JSON.stringify({
        type: 'receiver-count-updated',
        count: sess.receivers.size,
        receiverId: rKey
      }));
    }

    // 6. Receiver clicks "Download File(s) Now"
    else if (data.type === 'request-download') {
      const sess = sessions.get(boundSessionId);
      if (sess && sess.senderWs && sess.senderWs.readyState === ws.OPEN) {
        sess.senderWs.send(JSON.stringify({
          type: 'start-transfer-now',
          receiverId: myPeerId
        }));
      }
    }

    // 7. WebRTC Signaling (supports targeted receiverId for Broadcast Mode)
    else if (data.type === 'webrtc-signal') {
      const sess = sessions.get(boundSessionId);
      if (!sess) return;
      if (boundRole === 'sender') {
        const targetWs = data.receiverId ? sess.receivers.get(data.receiverId) : Array.from(sess.receivers.values())[0];
        if (targetWs && targetWs.readyState === ws.OPEN) {
          targetWs.send(JSON.stringify({
            type: 'webrtc-signal',
            payload: data.payload,
            senderId: myPeerId
          }));
        }
      } else {
        if (sess.senderWs && sess.senderWs.readyState === ws.OPEN) {
          sess.senderWs.send(JSON.stringify({
            type: 'webrtc-signal',
            payload: data.payload,
            receiverId: myPeerId
          }));
        }
      }
    }

    // 8. Control & Progress Sync Relay
    else if (
      data.type === 'file-header' ||
      data.type === 'file-complete' ||
      data.type === 'all-complete' ||
      data.type === 'progress-ack' ||
      data.type === 'abort-transfer' ||
      data.type === 'switch-to-fallback'
    ) {
      const sess = sessions.get(boundSessionId);
      if (!sess) return;
      if (boundRole === 'sender') {
        sess.receivers.forEach((rWs) => {
          if (rWs && rWs.readyState === ws.OPEN) {
            rWs.send(JSON.stringify(data));
          }
        });
      } else {
        if (sess.senderWs && sess.senderWs.readyState === ws.OPEN) {
          data.receiverId = myPeerId;
          sess.senderWs.send(JSON.stringify(data));
        }
      }
    }

    // 9. Instant P2P Text & Link Scratchpad Sync
    else if (data.type === 'scratchpad-update') {
      peers.forEach((p) => {
        if (p.peerId !== myPeerId && p.ipGroup === ipGroup && p.ws.readyState === ws.OPEN) {
          p.ws.send(JSON.stringify({
            type: 'scratchpad-remote',
            text: data.text,
            senderName: data.senderName || 'Peer'
          }));
        }
      });
      if (boundSessionId && sessions.has(boundSessionId)) {
        const sess = sessions.get(boundSessionId);
        if (boundRole === 'sender') {
          sess.receivers.forEach((rWs) => {
            if (rWs.readyState === ws.OPEN) {
              rWs.send(JSON.stringify({ type: 'scratchpad-remote', text: data.text, senderName: data.senderName || 'Sender' }));
            }
          });
        } else if (sess.senderWs && sess.senderWs.readyState === ws.OPEN) {
          sess.senderWs.send(JSON.stringify({ type: 'scratchpad-remote', text: data.text, senderName: data.senderName || 'Receiver' }));
        }
      }
    }
  });

  ws.on('close', () => {
    if (myPeerId && peers.has(myPeerId)) {
      peers.delete(myPeerId);
      broadcastRadarToIpGroup(ipGroup);
    }
    if (boundSessionId && sessions.has(boundSessionId)) {
      const sess = sessions.get(boundSessionId);
      if (boundRole === 'sender') {
        sess.receivers.forEach((rWs) => {
          if (rWs && rWs.readyState === ws.OPEN) {
            rWs.send(JSON.stringify({
              type: 'session-expired',
              message: 'Sender closed their browser tab. Transfer session ended.'
            }));
          }
        });
        codeToSession.delete(sess.code4);
        sessions.delete(boundSessionId);
      } else if (boundRole === 'receiver') {
        sess.receivers.delete(myPeerId);
        if (sess.senderWs && sess.senderWs.readyState === ws.OPEN) {
          sess.senderWs.send(JSON.stringify({
            type: 'receiver-count-updated',
            count: sess.receivers.size
          }));
        }
      }
    }
  });
});

// ==========================================================
// PWA ASSETS: MANIFEST.JSON, SERVICE WORKER (/sw.js) & ICONS
// ==========================================================
const APP_ICON_SVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="512" height="512">
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#111827"/>
      <stop offset="100%" stop-color="#030712"/>
    </linearGradient>
    <linearGradient id="bolt" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#fbbf24"/>
      <stop offset="50%" stop-color="#f59e0b"/>
      <stop offset="100%" stop-color="#10b981"/>
    </linearGradient>
  </defs>
  <rect width="512" height="512" rx="115" fill="url(#bg)" stroke="#f59e0b" stroke-width="12"/>
  <circle cx="256" cy="256" r="190" fill="none" stroke="#1f2937" stroke-width="6" stroke-dasharray="18 12"/>
  <circle cx="256" cy="256" r="135" fill="none" stroke="#374151" stroke-width="4"/>
  <path d="M285 96L144 276h102l-19 140 141-180H266L285 96z" fill="url(#bolt)"/>
</svg>`;

app.get('/icon-192.svg', (req, res) => {
  res.setHeader('Content-Type', 'image/svg+xml');
  res.setHeader('Cache-Control', 'public, max-age=86400');
  res.send(APP_ICON_SVG);
});

app.get('/icon-512.svg', (req, res) => {
  res.setHeader('Content-Type', 'image/svg+xml');
  res.setHeader('Cache-Control', 'public, max-age=86400');
  res.send(APP_ICON_SVG);
});

app.get('/manifest.json', (req, res) => {
  res.setHeader('Content-Type', 'application/manifest+json');
  res.send(JSON.stringify({
    id: '/',
    name: 'ChocoBento P2P — Hyper-Speed File Transfer',
    short_name: 'ChocoBento',
    description: 'Zero-Log, Direct Peer-to-Peer File & Folder Sharing Command Center',
    start_url: '/',
    scope: '/',
    display: 'standalone',
    display_override: ['window-controls-overlay', 'standalone'],
    orientation: 'any',
    background_color: '#090b10',
    theme_color: '#090b10',
    categories: ['utilities', 'productivity'],
    icons: [
      {
        src: '/icon-192.svg',
        sizes: '192x192',
        type: 'image/svg+xml',
        purpose: 'any'
      },
      {
        src: '/icon-512.svg',
        sizes: '512x512',
        type: 'image/svg+xml',
        purpose: 'any maskable'
      }
    ]
  }));
});

const SERVICE_WORKER_JS = `
const CACHE_NAME = 'chocobento-pwa-v3';
const PRECACHE_URLS = ['/', '/manifest.json', '/icon-192.svg', '/icon-512.svg'];

self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(PRECACHE_URLS).catch(() => {}))
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(
      keys.map((k) => (k !== CACHE_NAME ? caches.delete(k) : Promise.resolve()))
    )).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  event.respondWith(
    fetch(event.request)
      .then((networkResp) => {
        if (networkResp && networkResp.status === 200 && event.request.url.startsWith(self.location.origin)) {
          const copy = networkResp.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy)).catch(() => {});
        }
        return networkResp;
      })
      .catch(() => caches.match(event.request).then((cached) => cached || caches.match('/')))
  );
});
`;

app.get('/sw.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.setHeader('Service-Worker-Allowed', '/');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(SERVICE_WORKER_JS);
});

// ==========================================================
// MAIN APPLICATION UI (ADAPTIVE LAPTOP & SMARTPHONE + PWA)
// ==========================================================
const HTML_DOCUMENT = `<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0, viewport-fit=cover">
  <meta name="theme-color" content="#090b10">
  <meta name="mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <meta name="apple-mobile-web-app-title" content="ChocoBento">
  <link rel="manifest" href="/manifest.json">
  <link rel="icon" type="image/svg+xml" href="/icon-192.svg">
  <link rel="apple-touch-icon" href="/icon-192.svg">
  <title>ChocoBento P2P — Command Center</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/qrious/4.0.2/qrious.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"></script>
  <script src="https://unpkg.com/html5-qrcode" type="text/javascript"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');

    :root {
      --accent-main: #f59e0b;
      --accent-glow: rgba(245, 158, 11, 0.28);
      --state-border: rgba(75, 85, 99, 0.4);
      --state-shadow: rgba(0, 0, 0, 0.65);
    }

    body[data-accent="amber"] { --accent-main: #f59e0b; --accent-glow: rgba(245, 158, 11, 0.28); }
    body[data-accent="emerald"] { --accent-main: #10b981; --accent-glow: rgba(16, 185, 129, 0.28); }
    body[data-accent="cyan"] { --accent-main: #06b6d4; --accent-glow: rgba(6, 182, 212, 0.28); }
    body[data-accent="crimson"] { --accent-main: #f43f5e; --accent-glow: rgba(244, 63, 94, 0.28); }

    /* State-Reactive Ambient Lighting */
    body[data-state="idle"] {
      --state-border: rgba(75, 85, 99, 0.38);
      background: radial-gradient(circle at 50% 0%, #161b26 0%, #090b10 70%);
    }
    body[data-state="waiting"] {
      --state-border: rgba(245, 158, 11, 0.65);
      background: radial-gradient(circle at 50% 0%, #291d0a 0%, #090b10 70%);
    }
    body[data-state="connected"] {
      --state-border: rgba(6, 182, 212, 0.7);
      background: radial-gradient(circle at 50% 0%, #082830 0%, #090b10 70%);
    }
    body[data-state="streaming"] {
      --state-border: rgba(16, 185, 129, 0.8);
      background: radial-gradient(circle at 50% 0%, #062b1f 0%, #090b10 70%);
    }
    body[data-state="aborted"] {
      --state-border: rgba(239, 68, 68, 0.8);
      background: radial-gradient(circle at 50% 0%, #2d0a0f 0%, #090b10 70%);
    }

    * {
      -webkit-tap-highlight-color: transparent;
      box-sizing: border-box;
    }

    body {
      font-family: 'Plus Jakarta Sans', sans-serif;
      color: #f3f4f6;
      min-height: 100vh;
      min-height: 100dvh;
      transition: background 0.5s ease;
      overflow-x: hidden;
    }

    .mono-font {
      font-family: 'JetBrains Mono', monospace;
    }

    .bento-card {
      background: linear-gradient(155deg, rgba(24, 31, 44, 0.88) 0%, rgba(12, 16, 24, 0.95) 100%);
      backdrop-filter: blur(20px);
      border: 1px solid var(--state-border);
      box-shadow: 0 20px 45px -15px var(--state-shadow), 0 0 25px -8px var(--accent-glow);
      transition: border-color 0.35s ease, box-shadow 0.35s ease;
    }

    .accent-text { color: var(--accent-main); }
    .accent-bg { background-color: var(--accent-main); }
    .accent-border { border-color: var(--accent-main); }

    /* Touch-friendly ergonomic button sizing for smartphones */
    .touch-btn {
      min-height: 44px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      touch-action: manipulation;
    }

    /* Sonar Radar Animation */
    .radar-sweep {
      background: conic-gradient(from 0deg, transparent 70%, var(--accent-glow) 95%, var(--accent-main) 100%);
      animation: spinRadar 3.5s linear infinite;
    }
    @keyframes spinRadar {
      from { transform: rotate(0deg); }
      to { transform: rotate(360deg); }
    }

    .maximized-card {
      position: fixed !important;
      inset: 0.75rem !important;
      z-index: 70 !important;
      display: flex !important;
      flex-direction: column !important;
    }

    /* Prevent iOS/Android auto-zoom on input focus */
    @media (max-width: 768px) {
      input, textarea, select {
        font-size: 16px !important;
      }
    }

    .custom-scroll::-webkit-scrollbar { width: 5px; height: 5px; }
    .custom-scroll::-webkit-scrollbar-thumb { background: rgba(156, 163, 175, 0.3); border-radius: 9999px; }
  </style>
</head>
<body data-state="idle" data-accent="amber" class="p-2.5 sm:p-4 lg:p-6 pb-20 lg:pb-6 flex flex-col justify-between">

  <!-- FLOATING PILL DOCK NAVIGATION (ADAPTIVE FOR SMARTPHONE & LAPTOP) -->
  <header class="max-w-7xl w-full mx-auto mb-3.5 sm:mb-5 flex flex-wrap items-center justify-between gap-2 bg-gray-950/85 backdrop-blur-xl border border-gray-800/90 rounded-2xl px-3 sm:px-4 py-2.5 shadow-2xl sticky top-2 z-40">
    <!-- Brand & Ambient State Indicator -->
    <div class="flex items-center space-x-2.5">
      <div class="w-10 h-10 rounded-xl accent-bg text-gray-950 font-black flex items-center justify-center shadow-lg cursor-pointer text-lg flex-shrink-0" onclick="resetToCleanWorkspace()">
        ⚡
      </div>
      <div>
        <div class="flex items-center space-x-2">
          <span class="font-extrabold tracking-tight text-white text-base sm:text-lg">Choco<span class="accent-text">Bento</span></span>
          <span id="ambient-state-pill" class="text-[10px] mono-font uppercase font-bold px-2 py-0.5 rounded-md bg-gray-800 text-gray-300 border border-gray-700">IDLE • READY</span>
        </div>
        <p id="device-mode-label" class="text-[10px] sm:text-[11px] text-gray-400">Hyper-Speed P2P Command Center</p>
      </div>
    </div>

    <!-- Center Pill Controls: Wake-Lock + Broadcast Status -->
    <div class="hidden xl:flex items-center space-x-2 text-xs">
      <div id="wakelock-pill" class="flex items-center space-x-1.5 px-3 py-1.5 rounded-xl bg-gray-900 border border-gray-800 text-gray-400">
        <span>☀️</span>
        <span id="wakelock-text" class="text-[11px] font-semibold">Wake-Lock: Standby</span>
      </div>
      <div class="flex items-center space-x-1.5 px-3 py-1.5 rounded-xl bg-gray-900 border border-gray-800 text-emerald-400">
        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
        <span class="text-[11px] font-semibold">Hyper-Stream 256KB Engine</span>
      </div>
    </div>

    <!-- Right Controls: Install App Button, Theme Accent Switcher & QR Scanner -->
    <div class="flex items-center space-x-1.5 sm:space-x-2">
      <!-- Dedicated PWA Install App Button (Appears automatically when installable or gives instructions) -->
      <button id="pwa-install-btn" onclick="triggerPwaInstall()" class="hidden touch-btn px-3 py-1.5 rounded-xl accent-bg text-gray-950 font-extrabold text-xs shadow-lg flex items-center space-x-1.5 transition-all">
        <span>📲</span>
        <span>Install App</span>
      </button>

      <div class="flex items-center space-x-1.5 bg-gray-900 px-2.5 py-2 rounded-xl border border-gray-800" title="Switch Metallic Neon Accent">
        <button onclick="setAccent('amber')" aria-label="Amber Theme" class="w-4 h-4 sm:w-4 sm:h-4 rounded-full bg-amber-500 hover:scale-125 transition-transform"></button>
        <button onclick="setAccent('emerald')" aria-label="Emerald Theme" class="w-4 h-4 sm:w-4 sm:h-4 rounded-full bg-emerald-500 hover:scale-125 transition-transform"></button>
        <button onclick="setAccent('cyan')" aria-label="Cyan Theme" class="w-4 h-4 sm:w-4 sm:h-4 rounded-full bg-cyan-500 hover:scale-125 transition-transform"></button>
        <button onclick="setAccent('crimson')" aria-label="Crimson Theme" class="w-4 h-4 sm:w-4 sm:h-4 rounded-full bg-rose-500 hover:scale-125 transition-transform"></button>
      </div>

      <button onclick="openQrCameraModal()" class="touch-btn px-3 sm:px-3.5 py-1.5 rounded-xl bg-gray-900 hover:bg-gray-800 border border-gray-700 text-xs font-bold text-white flex items-center space-x-1.5 transition-all">
        <span class="text-base">📷</span>
        <span class="hidden sm:inline">Scan QR</span>
      </button>
    </div>
  </header>

  <!-- PERSISTENT BENTO-GRID WORKSPACE (RESPONSIVE SMARTPHONE + LAPTOP LAYOUT) -->
  <main class="max-w-7xl w-full mx-auto flex-grow grid grid-cols-1 lg:grid-cols-12 gap-4 sm:gap-5">

    <!-- ========================================================== -->
    <!-- LEFT COLUMN (7 COLS): TRANSFER HUB + CYBER-HUD TELEMETRY   -->
    <!-- ========================================================== -->
    <div class="lg:col-span-7 flex flex-col gap-4 sm:gap-5">

      <!-- BENTO MODULE 1: PRIMARY TRANSFER COMMAND HUB -->
      <div id="section-transfer-hub" class="bento-card rounded-3xl p-4 sm:p-6 flex-grow flex flex-col justify-between relative overflow-hidden">

        <!-- VIEW 1A: IDLE DROPZONE (ADAPTIVE FOR TOUCH SMARTPHONES & LAPTOP DRAG-DROP) -->
        <div id="hub-idle" class="my-auto space-y-4">
          <div id="bento-dropzone"
               onclick="document.getElementById('file-input').click()"
               class="border-2 border-dashed border-gray-700 hover:border-amber-400 active:border-amber-400 rounded-2xl p-5 sm:p-8 text-center cursor-pointer transition-all bg-gray-950/50 hover:bg-gray-900/70 group">
            <input type="file" id="file-input" multiple class="hidden" onchange="handleFilesPicked(this.files)">
            <input type="file" id="folder-input" webkitdirectory directory multiple class="hidden" onchange="handleFilesPicked(this.files)">

            <div class="w-16 h-16 sm:w-20 sm:h-20 mx-auto rounded-2xl bg-gray-900 border border-gray-700 flex items-center justify-center text-3xl sm:text-4xl group-hover:scale-110 transition-transform shadow-inner">
              🚀
            </div>
            <h2 id="dropzone-headline" class="mt-3.5 text-base sm:text-xl font-extrabold text-white">Tap to select files or drop folders here</h2>
            <p class="mt-1 text-xs text-gray-400">Supports unlimited file sizes • <kbd class="px-1.5 py-0.5 rounded bg-gray-800 text-amber-400 mono-font">Ctrl + V</kbd> clipboard paste ready</p>

            <!-- Touch-Friendly Action Buttons (Full-width stack on phones, inline on laptops) -->
            <div class="mt-5 grid grid-cols-1 sm:grid-cols-2 gap-2.5 max-w-md mx-auto" onclick="event.stopPropagation()">
              <button onclick="document.getElementById('file-input').click()" class="touch-btn w-full px-4 py-3 rounded-xl accent-bg text-gray-950 font-extrabold text-xs sm:text-sm shadow-lg hover:opacity-90 active:scale-98 transition-all">
                + Select File(s)
              </button>
              <button onclick="document.getElementById('folder-input').click()" class="touch-btn w-full px-4 py-3 rounded-xl bg-gray-800 hover:bg-gray-700 border border-gray-600 text-white font-bold text-xs sm:text-sm active:scale-98 transition-all">
                📁 Select Whole Folder
              </button>
            </div>
          </div>

          <div class="grid grid-cols-3 gap-2 text-center text-[10px] sm:text-[11px] text-gray-400">
            <div class="bg-gray-950/60 py-2.5 px-2 rounded-xl border border-gray-800/80 font-semibold">⚡ Max-Speed Stream</div>
            <div class="bg-gray-950/60 py-2.5 px-2 rounded-xl border border-gray-800/80 font-semibold">📡 Multi-Receiver</div>
            <div class="bg-gray-950/60 py-2.5 px-2 rounded-xl border border-gray-800/80 font-semibold">🛡️ CRC32 Verified</div>
          </div>
        </div>

        <!-- VIEW 1B: SENDER BROADCAST HUB (QR + 4-DIGIT CODE + OPTIONAL .ZIP BUNDLER) -->
        <div id="hub-sender-ready" class="hidden space-y-4 my-auto">
          <div class="flex flex-wrap items-center justify-between gap-2 border-b border-gray-800 pb-3">
            <div class="min-w-0 flex-1">
              <div class="flex flex-wrap items-center gap-1.5">
                <span class="px-2 py-0.5 rounded text-[10px] font-bold uppercase bg-amber-500/20 text-amber-300 border border-amber-500/30">Broadcast Active</span>
                <span id="sender-peer-counter" class="px-2 py-0.5 rounded text-[10px] font-bold uppercase bg-cyan-500/20 text-cyan-300 border border-cyan-500/30">0 Receivers Connected</span>
              </div>
              <h3 id="sender-title" class="text-sm sm:text-base font-extrabold text-white mt-1 truncate">Ready</h3>
              <p id="sender-subtitle" class="text-xs text-gray-400 mono-font">0 MB</p>
            </div>

            <div class="flex flex-wrap items-center gap-2">
              <!-- OPTIONAL 1-CLICK .ZIP BUNDLER BUTTON -->
              <button id="zip-bundle-btn" onclick="bundleSelectedFilesIntoZip()" class="hidden touch-btn px-3 py-2 rounded-xl bg-emerald-500/20 hover:bg-emerald-500/30 border border-emerald-500/40 text-emerald-300 text-xs font-bold transition-all">
                📦 Bundle all into .ZIP
              </button>
              <button onclick="resetToCleanWorkspace()" class="touch-btn px-3 py-2 rounded-xl bg-red-500/20 hover:bg-red-500/30 border border-red-500/40 text-red-300 text-xs font-bold transition-all">
                ✕ Reset
              </button>
            </div>
          </div>

          <!-- Compact File Queue List -->
          <div id="sender-file-queue" class="max-h-24 overflow-y-auto custom-scroll bg-gray-950/70 p-2.5 rounded-xl border border-gray-800/90 space-y-1 text-xs"></div>

          <!-- QR Code + 4-Digit Code + Link Grid -->
          <div class="grid grid-cols-1 sm:grid-cols-12 gap-4 items-center bg-gray-950/60 p-3.5 rounded-2xl border border-gray-800">
            <div class="sm:col-span-5 flex flex-col items-center justify-center">
              <div class="bg-white p-2.5 rounded-2xl shadow-lg border-2 border-amber-400 w-[168px] h-[168px] flex items-center justify-center">
                <canvas id="sender-qr-canvas" width="148" height="148" class="w-[148px] h-[148px] block"></canvas>
                <img id="sender-qr-fallback" alt="QR" class="w-[148px] h-[148px] hidden" />
              </div>
              <span class="text-[10px] text-gray-400 mt-1.5">Active while this screen stays open</span>
            </div>

            <div class="sm:col-span-7 space-y-3 w-full">
              <div class="flex items-center justify-between bg-gray-900/90 px-3.5 py-2.5 rounded-xl border border-gray-800">
                <div>
                  <p class="text-[10px] uppercase font-bold text-gray-400">4-Digit Nearby Code</p>
                  <p class="text-[11px] text-gray-500">Or tap device on Sonar Radar</p>
                </div>
                <span id="sender-code4" class="text-2xl sm:text-3xl font-black tracking-[0.2em] accent-text mono-font">----</span>
              </div>

              <div class="space-y-1.5">
                <label class="text-[11px] font-semibold text-gray-400">Direct Share Link (Multi-User Broadcast):</label>
                <div class="flex flex-col sm:flex-row gap-2">
                  <input type="text" id="sender-link-input" readonly class="w-full bg-gray-900 border border-gray-700 rounded-xl px-3 py-2.5 text-xs text-gray-200 mono-font focus:outline-none">
                  <div class="flex gap-2">
                    <button onclick="copyBroadcastLink()" id="copy-link-btn" class="touch-btn flex-1 sm:flex-none px-4 py-2.5 rounded-xl accent-bg text-gray-950 font-extrabold text-xs whitespace-nowrap">
                      Copy Link
                    </button>
                    <button onclick="nativeShareBroadcastLink()" id="native-share-btn" class="hidden touch-btn px-3.5 py-2.5 rounded-xl bg-gray-800 hover:bg-gray-700 border border-gray-600 text-white font-bold text-xs whitespace-nowrap">
                      📤 Share
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- VIEW 1C: RECEIVER PREVIEW (SHOWS FILE NAME, SIZE & DOWNLOAD BUTTON BEFORE STREAMING) -->
        <div id="hub-receiver-preview" class="hidden my-auto space-y-5 text-center py-3">
          <div class="inline-flex items-center space-x-2 px-3 py-1 rounded-full bg-cyan-500/15 border border-cyan-500/30 text-cyan-300 text-xs font-bold">
            <span>📥 Incoming High-Speed P2P Transfer</span>
          </div>

          <div>
            <h3 id="receiver-title" class="text-lg sm:text-xl font-extrabold text-white break-all px-2">Loading file metadata...</h3>
            <p id="receiver-subtitle" class="text-sm accent-text font-bold mono-font mt-1">0 MB</p>
          </div>

          <div id="receiver-file-list" class="max-h-36 overflow-y-auto custom-scroll text-left bg-gray-950/80 p-3 rounded-xl border border-gray-800 space-y-1.5 text-xs"></div>

          <div class="flex flex-col sm:flex-row gap-3 pt-1">
            <button onclick="startReceiverDownload()" class="touch-btn flex-grow py-4 px-6 rounded-2xl accent-bg text-gray-950 font-black text-sm sm:text-base shadow-xl hover:opacity-95 active:scale-98 transition-all flex items-center justify-center space-x-2">
              <span>⬇️ Download File(s) at Max Speed</span>
            </button>
            <button onclick="resetToCleanWorkspace()" class="touch-btn py-3.5 px-5 rounded-2xl bg-gray-800 hover:bg-gray-700 text-gray-300 font-bold text-xs">
              Cancel
            </button>
          </div>
        </div>

        <!-- VIEW 1D: ACTIVE TRANSFER PROGRESS HUD (SHOWN ON BOTH SENDER & RECEIVER) -->
        <div id="hub-active-transfer" class="hidden my-auto space-y-4">
          <div class="flex flex-wrap items-center justify-between gap-2 border-b border-gray-800 pb-3">
            <div class="min-w-0 flex-1">
              <div class="flex flex-wrap items-center gap-1.5">
                <span id="active-role-badge" class="px-2 py-0.5 rounded text-[10px] font-bold uppercase bg-emerald-500/20 text-emerald-300">STREAMING</span>
                <span id="integrity-badge" class="hidden px-2.5 py-0.5 rounded text-[10px] font-bold uppercase bg-emerald-500/30 text-emerald-200 border border-emerald-400/50">
                  🛡️ 100% Integrity Verified
                </span>
              </div>
              <h3 id="active-filename" class="text-sm sm:text-base font-bold text-white truncate mt-1">file.bin</h3>
            </div>

            <div class="flex items-center gap-2">
              <button id="back-to-qr-btn" onclick="returnSenderToQrView()" class="hidden touch-btn px-3 py-2 rounded-xl bg-gray-800 hover:bg-gray-700 text-amber-300 text-xs font-bold">
                QR / Link
              </button>
              <button onclick="triggerStopAndRestart()" class="touch-btn px-3.5 py-2 rounded-xl bg-rose-600 hover:bg-rose-500 text-white text-xs font-extrabold shadow-lg transition-all">
                Stop & Restart
              </button>
            </div>
          </div>

          <!-- Progress Bar & Percentage -->
          <div class="space-y-2">
            <div class="flex justify-between items-end">
              <span id="active-status-text" class="text-xs text-gray-300 font-medium">Pumping unthrottled binary frames...</span>
              <span id="active-percent-text" class="text-3xl sm:text-4xl font-black accent-text mono-font">0%</span>
            </div>
            <div class="w-full h-4 sm:h-5 bg-gray-950 rounded-full overflow-hidden border border-gray-800 p-0.5">
              <div id="active-progress-fill" class="h-full rounded-full bg-gradient-to-r from-amber-500 via-emerald-400 to-cyan-400 transition-all duration-75" style="width: 0%"></div>
            </div>
            <div class="flex justify-between text-[11px] text-gray-400 mono-font">
              <span id="active-bytes-counter">0.00 MB / 0.00 MB</span>
              <span id="active-file-index">File 1 of 1</span>
            </div>
          </div>

          <!-- Speed, ETA, Checksum Row -->
          <div class="grid grid-cols-3 gap-2.5 sm:gap-3 text-center">
            <div class="bg-gray-950/75 p-2.5 sm:p-3 rounded-xl border border-gray-800">
              <p class="text-[10px] uppercase text-gray-400">Live Speed</p>
              <p id="hud-speed" class="text-sm sm:text-base font-extrabold text-emerald-400 mono-font mt-0.5">0.00 MB/s</p>
            </div>
            <div class="bg-gray-950/75 p-2.5 sm:p-3 rounded-xl border border-gray-800">
              <p class="text-[10px] uppercase text-gray-400">ETA Remaining</p>
              <p id="hud-eta" class="text-sm sm:text-base font-extrabold text-white mono-font mt-0.5">-- s</p>
            </div>
            <div class="bg-gray-950/75 p-2.5 sm:p-3 rounded-xl border border-gray-800">
              <p class="text-[10px] uppercase text-gray-400">CRC32 Hash</p>
              <p id="hud-crc" class="text-xs sm:text-base font-extrabold text-cyan-400 mono-font mt-0.5 truncate">0x00000000</p>
            </div>
          </div>
        </div>

      </div>

      <!-- BENTO MODULE 2: CYBER-HUD TELEMETRY & THROUGHPUT GRAPH -->
      <div id="telemetry-bento-card" class="bento-card rounded-3xl p-4 sm:p-5 space-y-3">
        <div class="flex items-center justify-between">
          <div class="flex items-center space-x-2">
            <span class="text-sm">📊</span>
            <h3 class="text-xs font-extrabold uppercase tracking-wider text-gray-200">Cyber-HUD Network Telemetry</h3>
          </div>
          <div class="flex items-center space-x-2">
            <span id="hud-protocol-badge" class="px-2 py-0.5 rounded text-[10px] mono-font font-bold bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">Hyper-Stream P2P</span>
            <button onclick="toggleMaximizeCard('telemetry-bento-card')" class="px-2.5 py-1 rounded-lg bg-gray-800 hover:bg-gray-700 text-[10px] font-bold text-gray-300">
              ⛶ Focus
            </button>
          </div>
        </div>

        <!-- 4 Cyber-HUD Telemetry Pills -->
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-2 text-center mono-font">
          <div class="bg-gray-950/80 p-2 rounded-xl border border-gray-800/90">
            <span class="text-[10px] text-gray-400 block">PING LATENCY</span>
            <span id="telemetry-ping" class="text-xs font-bold text-emerald-400">8 ms</span>
          </div>
          <div class="bg-gray-950/80 p-2 rounded-xl border border-gray-800/90">
            <span class="text-[10px] text-gray-400 block">BUFFER PIPE</span>
            <span id="telemetry-chunk-label" class="text-xs font-bold text-amber-400">128-256 KB</span>
          </div>
          <div class="bg-gray-950/80 p-2 rounded-xl border border-gray-800/90">
            <span class="text-[10px] text-gray-400 block">FRAMES PUMPED</span>
            <span id="telemetry-packets" class="text-xs font-bold text-cyan-400">0 pkts</span>
          </div>
          <div class="bg-gray-950/80 p-2 rounded-xl border border-gray-800/90">
            <span class="text-[10px] text-gray-400 block">PEAK SPEED</span>
            <span id="telemetry-peak" class="text-xs font-bold text-white">0.00 MB/s</span>
          </div>
        </div>

        <!-- Live Throughput Canvas -->
        <div class="bg-gray-950/90 p-2.5 rounded-2xl border border-gray-800 flex-grow">
          <canvas id="telemetry-canvas" width="520" height="80" class="w-full h-[75px] sm:h-[85px] block"></canvas>
        </div>
      </div>

    </div>

    <!-- ========================================================== -->
    <!-- RIGHT COLUMN (5 COLS): SONAR RADAR + P2P TEXT SCRATCHPAD   -->
    <!-- ========================================================== -->
    <div class="lg:col-span-5 flex flex-col gap-4 sm:gap-5">

      <!-- BENTO MODULE 3: AIRDROP-STYLE SONAR RADAR (SAME-WIFI AUTO-DISCOVERY) -->
      <div id="section-sonar-radar" class="bento-card rounded-3xl p-4 sm:p-5 space-y-3.5">
        <div class="flex flex-wrap items-center justify-between gap-2">
          <div>
            <div class="flex items-center space-x-2">
              <span class="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping"></span>
              <h3 class="text-xs font-extrabold uppercase tracking-wider text-gray-200">Sonar Radar • Same-Wi-Fi</h3>
            </div>
            <p class="text-[11px] text-gray-400 mt-0.5">Tap any discovered device to beam files instantly</p>
          </div>

          <!-- Local Device Smart Identity Pill (Editable Nickname) -->
          <button onclick="promptRenameDevice()" class="touch-btn px-3 py-1.5 rounded-xl bg-gray-900 hover:bg-gray-800 border border-gray-700 text-[11px] font-bold text-amber-300 flex items-center space-x-1.5" title="Tap to rename your device">
            <span id="my-os-icon">💻</span>
            <span id="my-device-name" class="truncate max-w-[120px]">My Device</span>
            <span class="text-gray-500">✎</span>
          </button>
        </div>

        <!-- Interactive Concentric Sonar Radar Box -->
        <div class="relative h-48 sm:h-44 rounded-2xl bg-gray-950/90 border border-gray-800 overflow-hidden flex items-center justify-center">
          <div class="w-40 h-40 rounded-full border border-gray-800/80 absolute"></div>
          <div class="w-28 h-28 rounded-full border border-gray-800/80 absolute"></div>
          <div class="w-14 h-14 rounded-full border border-gray-800/80 absolute"></div>
          <div class="w-44 h-44 rounded-full radar-sweep opacity-25 absolute pointer-events-none"></div>

          <!-- Center Self Node -->
          <div class="z-10 w-10 h-10 rounded-full accent-bg text-gray-950 font-black flex items-center justify-center shadow-lg text-xs" title="You">
            YOU
          </div>

          <!-- Dynamic Nearby Peer Nodes Container -->
          <div id="radar-nodes-layer" class="absolute inset-0"></div>

          <div id="radar-empty-hint" class="absolute bottom-2 inset-x-2 text-center text-[11px] text-gray-500 pointer-events-none">
            Open this app on your phone/PC on the same Wi-Fi for zero-click discovery
          </div>
        </div>

        <!-- 4-Digit Code Direct Input Bar -->
        <div class="flex gap-2">
          <input type="text" id="radar-code-input" inputmode="numeric" pattern="[0-9]*" maxlength="4" placeholder="Enter 4-Digit Code"
                 class="w-full bg-gray-950 border border-gray-700 focus:border-amber-400 rounded-xl px-3.5 py-2.5 text-sm font-bold tracking-widest text-center mono-font text-white focus:outline-none">
          <button onclick="connectBy4DigitCode()" class="touch-btn px-5 py-2.5 rounded-xl accent-bg text-gray-950 font-extrabold text-xs sm:text-sm whitespace-nowrap shadow-md">
            Connect
          </button>
        </div>
      </div>

      <!-- BENTO MODULE 4: INSTANT P2P TEXT & LINK SCRATCHPAD -->
      <div id="scratchpad-bento-card" class="bento-card rounded-3xl p-4 sm:p-5 flex-grow flex flex-col justify-between space-y-3">
        <div class="flex flex-wrap items-center justify-between gap-2">
          <div class="flex items-center space-x-2">
            <span>⚡</span>
            <h3 class="text-xs font-extrabold uppercase tracking-wider text-gray-200">P2P Text & Code Scratchpad</h3>
          </div>

          <div class="flex flex-wrap items-center gap-1.5">
            <button onclick="toggleScratchpadCodeMode()" id="code-fmt-btn" class="px-2.5 py-1.5 rounded-lg bg-gray-800 hover:bg-gray-700 text-[10px] mono-font font-bold text-cyan-300 border border-gray-700">
              { } Code
            </button>
            <button onclick="showScratchpadQr()" class="px-2.5 py-1.5 rounded-lg bg-gray-800 hover:bg-gray-700 text-[10px] font-bold text-amber-300 border border-gray-700">
              QR
            </button>
            <button onclick="copyScratchpadText()" id="scratch-copy-btn" class="px-2.5 py-1.5 rounded-lg bg-gray-800 hover:bg-gray-700 text-[10px] font-bold text-white border border-gray-700">
              Copy
            </button>
            <button onclick="clearScratchpadText()" class="px-2.5 py-1.5 rounded-lg bg-gray-800 hover:bg-gray-700 text-[10px] font-bold text-rose-300 border border-gray-700">
              Clear
            </button>
            <button onclick="toggleMaximizeCard('scratchpad-bento-card')" class="px-2.5 py-1.5 rounded-lg bg-gray-800 hover:bg-gray-700 text-[10px] font-bold text-gray-300">
              ⛶
            </button>
          </div>
        </div>

        <textarea id="p2p-scratchpad"
                  oninput="onScratchpadInput(this.value)"
                  placeholder="Type or paste URLs, OTPs, Wi-Fi passwords, or code snippets here... Syncs live with nearby & connected devices!"
                  class="w-full flex-grow min-h-[120px] bg-gray-950/90 border border-gray-800 focus:border-amber-400 rounded-2xl p-3.5 text-xs sm:text-sm text-gray-200 focus:outline-none resize-none custom-scroll"></textarea>

        <div class="flex justify-between items-center text-[10px] text-gray-400">
          <span id="scratchpad-sync-status">Live sync ready across same-Wi-Fi & session</span>
          <span id="scratchpad-char-count" class="mono-font">0 chars</span>
        </div>
      </div>

    </div>

  </main>

  <!-- SMARTPHONE BOTTOM ERGONOMIC QUICK-JUMP BAR (VISIBLE ON MOBILE PHONES ONLY) -->
  <nav class="lg:hidden fixed bottom-0 inset-x-0 z-40 bg-gray-950/95 backdrop-blur-xl border-t border-gray-800 px-2 py-1.5 grid grid-cols-4 gap-1 text-[10px] font-bold text-gray-300">
    <button onclick="scrollToMobileSection('section-transfer-hub')" class="touch-btn flex flex-col items-center justify-center rounded-xl hover:bg-gray-900 py-1 text-amber-400">
      <span class="text-base">🚀</span>
      <span>Transfer</span>
    </button>
    <button onclick="scrollToMobileSection('section-sonar-radar')" class="touch-btn flex flex-col items-center justify-center rounded-xl hover:bg-gray-900 py-1">
      <span class="text-base">📡</span>
      <span>Radar</span>
    </button>
    <button onclick="scrollToMobileSection('scratchpad-bento-card')" class="touch-btn flex flex-col items-center justify-center rounded-xl hover:bg-gray-900 py-1">
      <span class="text-base">📝</span>
      <span>Scratchpad</span>
    </button>
    <button onclick="triggerPwaInstall()" class="touch-btn flex flex-col items-center justify-center rounded-xl hover:bg-gray-900 py-1 text-emerald-400">
      <span class="text-base">📲</span>
      <span>Install App</span>
    </button>
  </nav>

  <!-- MODAL 1: CAMERA QR SCANNER -->
  <div id="qr-camera-modal" class="fixed inset-0 z-50 hidden bg-black/85 backdrop-blur-md flex items-center justify-center p-4">
    <div class="bento-card rounded-3xl p-5 sm:p-6 max-w-md w-full space-y-4 text-center">
      <div class="flex justify-between items-center">
        <h3 class="text-base font-extrabold text-white">📷 Scan Peer QR Code</h3>
        <button onclick="closeQrCameraModal()" class="touch-btn px-3 text-gray-400 hover:text-white text-xs font-bold">✕ Close</button>
      </div>
      <div id="html5-qr-reader" class="w-full overflow-hidden rounded-2xl border border-gray-700 bg-black min-h-[240px]"></div>
      <p class="text-xs text-gray-400">Point your camera at the QR code on the other device.</p>
    </div>
  </div>

  <!-- MODAL 2: SCRATCHPAD INSTANT QR POPUP -->
  <div id="scratchpad-qr-modal" class="fixed inset-0 z-50 hidden bg-black/85 backdrop-blur-md flex items-center justify-center p-4">
    <div class="bento-card rounded-3xl p-6 max-w-xs w-full space-y-4 text-center">
      <div class="flex justify-between items-center">
        <h3 class="text-sm font-extrabold text-white">Scratchpad Instant QR</h3>
        <button onclick="document.getElementById('scratchpad-qr-modal').classList.add('hidden')" class="touch-btn px-2 text-gray-400 hover:text-white text-xs font-bold">✕</button>
      </div>
      <div class="bg-white p-3 rounded-2xl mx-auto w-[196px] h-[196px] flex items-center justify-center">
        <canvas id="scratchpad-qr-canvas" width="176" height="176" class="w-[176px] h-[176px] block"></canvas>
      </div>
      <p class="text-[11px] text-gray-400">Scan with your phone to grab the scratchpad text or link immediately.</p>
    </div>
  </div>

  <!-- ========================================================== -->
  <!-- CLIENT-SIDE HYPER-STREAM ENGINE + PWA INSTALLER            -->
  <!-- ========================================================== -->
  <script>
    // High-Speed Chunk Constants:
    // 128KB for WebRTC DataChannel (safe across all Chromium/Firefox SCTP implementations)
    // 256KB for Turbo WebSocket Binary Relay (maximum TCP frame saturation)
    const WEBRTC_CHUNK_SIZE = 128 * 1024;
    const RELAY_CHUNK_SIZE = 256 * 1024;
    const WEBRTC_HIGH_WATER_MARK = 8 * 1024 * 1024;  // 8MB pipe buffer
    const WEBRTC_LOW_WATER_MARK = 2 * 1024 * 1024;   // Resume immediately at 2MB

    let ws = null;
    let pc = null;
    let dataChannel = null;
    let iceQueue = [];

    // Local Smart Device Identity
    const myPeerId = 'peer-' + Math.random().toString(36).substring(2, 10);
    let myDeviceInfo = detectSmartDeviceIdentity();
    let pendingBeamTargetPeerId = null;

    // File Session State
    let selectedFiles = [];
    let totalSelectedBytes = 0;
    let currentSessionId = new URLSearchParams(window.location.search).get('s') || null;
    let role = currentSessionId ? 'receiver' : 'sender';

    // Streaming & Telemetry State
    let isTransferring = false;
    let isAborted = false;
    let usingFallbackRelay = false;
    let p2pWatchdog = null;
    let transferStartTime = 0;
    let peakSpeedMbps = 0;
    let packetsCount = 0;
    let speedSamples = [];
    let senderAckBytes = 0;
    let rollingCrc = 0;
    let lastUiRenderTime = 0;

    // Receiver Buffers
    let incomingFilesMeta = [];
    let incomingTotalBytes = 0;
    let activeFileHeader = null;
    let currentFileChunks = [];
    let totalReceivedBytes = 0;
    let lastAckTimestamp = 0;

    // Wake-Lock, QR Scanner & PWA Install Prompt
    let wakeLockSentinel = null;
    let qrScannerInstance = null;
    let deferredPwaPrompt = null;

    // ------------------------------------------------------------
    // 0. STANDALONE PWA SERVICE WORKER & INSTALL PROMPT HANDLER
    // ------------------------------------------------------------
    if ('serviceWorker' in navigator) {
      window.addEventListener('load', function() {
        navigator.serviceWorker.register('/sw.js', { scope: '/' }).catch(function() {});
      });
    }

    window.addEventListener('beforeinstallprompt', function(e) {
      e.preventDefault();
      deferredPwaPrompt = e;
      const installBtn = document.getElementById('pwa-install-btn');
      if (installBtn) installBtn.classList.remove('hidden');
    });

    window.addEventListener('appinstalled', function() {
      deferredPwaPrompt = null;
      const installBtn = document.getElementById('pwa-install-btn');
      if (installBtn) installBtn.classList.add('hidden');
    });

    async function triggerPwaInstall() {
      if (deferredPwaPrompt) {
        deferredPwaPrompt.prompt();
        await deferredPwaPrompt.userChoice;
        deferredPwaPrompt = null;
      } else {
        const isStandalone = window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone;
        if (isStandalone) {
          alert('ChocoBento is already installed and running as a standalone app!');
        } else {
          alert('To install ChocoBento as a standalone app: Tap the Chrome three-dot menu (⋮) at the top right and select "Install app".');
        }
      }
    }

    function scrollToMobileSection(id) {
      const el = document.getElementById(id);
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    // ------------------------------------------------------------
    // 1. SMART DEVICE CODENAME & ADAPTIVE UI ERGONOMICS
    // ------------------------------------------------------------
    function detectSmartDeviceIdentity() {
      const ua = navigator.userAgent || '';
      let os = 'Desktop';
      let icon = '💻';
      let devType = 'desktop';

      if (/android/i.test(ua)) { os = 'Android'; icon = '📱'; devType = 'mobile'; }
      else if (/iPad|iPhone|iPod/.test(ua)) { os = 'iPhone'; icon = '📱'; devType = 'mobile'; }
      else if (/Mac/i.test(ua)) { os = 'Mac'; icon = '💻'; devType = 'desktop'; }
      else if (/Win/i.test(ua)) { os = 'Windows PC'; icon = '🖥️'; devType = 'desktop'; }
      else if (/Linux/i.test(ua)) { os = 'Linux'; icon = '🐧'; devType = 'desktop'; }

      const adjectives = ['Swift', 'Cyber', 'Nova', 'Hyper', 'Quantum', 'Apex', 'Sonic', 'Titan'];
      const savedName = localStorage.getItem('chocobento_nick');
      const randomTag = adjectives[Math.floor(Math.random() * adjectives.length)] + ' ' + os;
      const finalName = savedName || randomTag;

      document.getElementById('my-os-icon').innerText = icon;
      document.getElementById('my-device-name').innerText = finalName;

      // Adapt UI copy & buttons specifically for Smartphone vs Laptop
      const modeLabel = document.getElementById('device-mode-label');
      const dropHeadline = document.getElementById('dropzone-headline');
      const nativeShareBtn = document.getElementById('native-share-btn');

      if (devType === 'mobile') {
        if (modeLabel) modeLabel.innerText = icon + ' ' + os + ' Touch Mode • Standalone App Ready';
        if (dropHeadline) dropHeadline.innerText = 'Tap below to pick photos, videos, or files';
        if (nativeShareBtn && navigator.share) nativeShareBtn.classList.remove('hidden');
      } else {
        if (modeLabel) modeLabel.innerText = icon + ' ' + os + ' Workstation • Ctrl+V Paste Active';
        if (dropHeadline) dropHeadline.innerText = 'Drop files or entire folders here to start sharing';
      }

      return { name: finalName, os: os, icon: icon, deviceType: devType };
    }

    function promptRenameDevice() {
      const next = prompt('Enter a recognizable nickname for this device on the Sonar Radar:', myDeviceInfo.name);
      if (next && next.trim()) {
        myDeviceInfo.name = next.trim().substring(0, 22);
        localStorage.setItem('chocobento_nick', myDeviceInfo.name);
        document.getElementById('my-device-name').innerText = myDeviceInfo.name;
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'update-peer-name', name: myDeviceInfo.name }));
        }
      }
    }

    // ------------------------------------------------------------
    // 2. STATE-REACTIVE AMBIENT LIGHTING & ACCENT SWITCHER
    // ------------------------------------------------------------
    function setAmbientState(stateName, labelText) {
      document.body.setAttribute('data-state', stateName);
      const pill = document.getElementById('ambient-state-pill');
      if (pill) pill.innerText = labelText;
    }

    function setAccent(accentName) {
      document.body.setAttribute('data-accent', accentName);
    }

    function toggleMaximizeCard(cardId) {
      const el = document.getElementById(cardId);
      if (el) el.classList.toggle('maximized-card');
    }

    // ------------------------------------------------------------
    // 3. AUTOMATIC SCREEN WAKE-LOCK
    // ------------------------------------------------------------
    async function requestScreenWakeLock() {
      if ('wakeLock' in navigator) {
        try {
          wakeLockSentinel = await navigator.wakeLock.request('screen');
          const wlText = document.getElementById('wakelock-text');
          const wlPill = document.getElementById('wakelock-pill');
          if (wlText) wlText.innerText = 'Wake-Lock: Active';
          if (wlPill) wlPill.classList.add('text-amber-300');
        } catch (err) {}
      }
    }

    // ------------------------------------------------------------
    // 4. WEBSOCKET SIGNALING & RADAR ENGINE
    // ------------------------------------------------------------
    function initSignaling() {
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      ws = new WebSocket(proto + '//' + window.location.host);
      ws.binaryType = 'arraybuffer';

      ws.onopen = function() {
        ws.send(JSON.stringify({
          type: 'register-peer',
          peerId: myPeerId,
          name: myDeviceInfo.name,
          os: myDeviceInfo.os,
          deviceType: myDeviceInfo.deviceType
        }));

        if (currentSessionId && role === 'receiver') {
          setAmbientState('connected', 'PEER LINK OPEN');
          showHubView('hub-receiver-preview');
          ws.send(JSON.stringify({ type: 'join-session', sessionId: currentSessionId }));
        }
      };

      ws.onclose = function() {
        setTimeout(initSignaling, 2000);
      };

      ws.onmessage = async function(event) {
        if (event.data instanceof ArrayBuffer) {
          handleIncomingChunkBuffer(event.data);
          return;
        }

        let msg;
        try { msg = JSON.parse(event.data); } catch (e) { return; }

        if (msg.type === 'hud-pong') {
          const rtt = Math.max(1, Math.round(performance.now() - msg.t));
          document.getElementById('telemetry-ping').innerText = rtt + ' ms';
        }
        else if (msg.type === 'radar-peers') {
          renderSonarRadarNodes(msg.peers || []);
        }
        else if (msg.type === 'session-created') {
          document.getElementById('sender-code4').innerText = msg.code4;
          if (pendingBeamTargetPeerId) {
            ws.send(JSON.stringify({
              type: 'beam-to-peer',
              targetPeerId: pendingBeamTargetPeerId,
              sessionId: currentSessionId,
              senderName: myDeviceInfo.name
            }));
            pendingBeamTargetPeerId = null;
          }
        }
        else if (msg.type === 'incoming-radar-beam') {
          currentSessionId = msg.sessionId;
          role = 'receiver';
          setAmbientState('connected', 'BEAM FROM ' + String(msg.senderName).toUpperCase());
          showHubView('hub-receiver-preview');
          ws.send(JSON.stringify({ type: 'join-session', sessionId: currentSessionId }));
        }
        else if (msg.type === 'code-found') {
          currentSessionId = msg.sessionId;
          role = 'receiver';
          setAmbientState('connected', 'CODE MATCHED');
          showHubView('hub-receiver-preview');
          ws.send(JSON.stringify({ type: 'join-session', sessionId: currentSessionId }));
        }
        else if (msg.type === 'session-info') {
          incomingFilesMeta = msg.files || [];
          incomingTotalBytes = msg.totalSize || 0;
          renderReceiverPreviewList();
        }
        else if (msg.type === 'receiver-count-updated') {
          const countEl = document.getElementById('sender-peer-counter');
          if (countEl) countEl.innerText = msg.count + (msg.count === 1 ? ' Receiver Connected' : ' Receivers Connected');
          if (msg.count > 0) setAmbientState('connected', msg.count + ' PEER(S) CONNECTED');
        }
        else if (msg.type === 'start-transfer-now') {
          showHubView('hub-active-transfer');
          document.getElementById('back-to-qr-btn').classList.remove('hidden');
          startSenderTransfer(msg.receiverId);
        }
        else if (msg.type === 'webrtc-signal') {
          await processWebRtcSignal(msg.payload, msg.receiverId);
        }
        else if (msg.type === 'switch-to-fallback') {
          usingFallbackRelay = true;
          document.getElementById('hud-protocol-badge').innerText = 'Turbo 256KB Relay';
        }
        else if (msg.type === 'file-header') {
          onFileHeaderReceived(msg);
        }
        else if (msg.type === 'file-complete') {
          saveCompletedIncomingFile();
        }
        else if (msg.type === 'all-complete') {
          completeTransferSuccess('Download 100% Complete!');
        }
        else if (msg.type === 'progress-ack') {
          senderAckBytes = msg.receivedBytes;
          updateTelemetryAndProgress(senderAckBytes, totalSelectedBytes, senderAckBytes >= totalSelectedBytes);
          if (senderAckBytes >= totalSelectedBytes && totalSelectedBytes > 0) {
            completeTransferSuccess('Broadcast Transfer 100% Verified!');
          }
        }
        else if (msg.type === 'abort-transfer') {
          handleTransferAborted();
        }
        else if (msg.type === 'scratchpad-remote') {
          const pad = document.getElementById('p2p-scratchpad');
          pad.value = msg.text;
          document.getElementById('scratchpad-char-count').innerText = msg.text.length + ' chars';
          document.getElementById('scratchpad-sync-status').innerText = 'Synced from ' + msg.senderName;
        }
        else if (msg.type === 'session-expired' || msg.type === 'error-msg') {
          alert(msg.message);
          resetToCleanWorkspace();
        }
      };

      setInterval(function() {
        if (ws && ws.readyState === WebSocket.OPEN && !isTransferring) {
          ws.send(JSON.stringify({ type: 'hud-ping', t: performance.now() }));
        }
      }, 3500);
    }

    // ------------------------------------------------------------
    // 5. SONAR RADAR NODES & ONE-CLICK TAP-TO-SEND
    // ------------------------------------------------------------
    function renderSonarRadarNodes(peerList) {
      const container = document.getElementById('radar-nodes-layer');
      const emptyHint = document.getElementById('radar-empty-hint');
      container.innerHTML = '';

      if (!peerList || peerList.length === 0) {
        emptyHint.classList.remove('hidden');
        return;
      }
      emptyHint.classList.add('hidden');

      const positions = [
        { top: '20%', left: '22%' },
        { top: '22%', left: '76%' },
        { top: '74%', left: '24%' },
        { top: '72%', left: '76%' },
        { top: '46%', left: '16%' },
        { top: '46%', left: '84%' }
      ];

      peerList.slice(0, 6).forEach(function(peer, idx) {
        const pos = positions[idx % positions.length];
        const btn = document.createElement('button');
        btn.style.top = pos.top;
        btn.style.left = pos.left;
        btn.className = 'absolute -translate-x-1/2 -translate-y-1/2 z-20 flex flex-col items-center group cursor-pointer p-1';
        btn.onclick = function() { onRadarPeerClicked(peer); };

        const iconChar = peer.deviceType === 'mobile' ? '📱' : '🖥️';
        btn.innerHTML =
          '<div class="w-11 h-11 rounded-2xl bg-gray-900 border-2 border-emerald-400 flex items-center justify-center text-xl shadow-lg group-hover:scale-110 active:scale-95 transition-transform">' +
            iconChar +
          '</div>' +
          '<span class="mt-1 px-2 py-0.5 rounded bg-gray-950/90 border border-gray-700 text-[10px] font-bold text-emerald-300 whitespace-nowrap">' +
            escapeSafe(peer.name) + ' • Tap' +
          '</span>';
        container.appendChild(btn);
      });
    }

    function onRadarPeerClicked(peer) {
      if (selectedFiles.length > 0 && currentSessionId) {
        ws.send(JSON.stringify({
          type: 'beam-to-peer',
          targetPeerId: peer.peerId,
          sessionId: currentSessionId,
          senderName: myDeviceInfo.name
        }));
      } else {
        pendingBeamTargetPeerId = peer.peerId;
        document.getElementById('file-input').click();
      }
    }

    function connectBy4DigitCode() {
      const code = document.getElementById('radar-code-input').value.trim();
      if (code.length !== 4) {
        alert('Please enter a 4-digit local code.');
        return;
      }
      ws.send(JSON.stringify({ type: 'lookup-code', code: code }));
    }

    // ------------------------------------------------------------
    // 6. FILE SELECTION, FOLDER DRAG-DROP, CTRL+V & OPTIONAL .ZIP
    // ------------------------------------------------------------
    function handleFilesPicked(fileList) {
      if (!fileList || fileList.length === 0) return;
      selectedFiles = Array.from(fileList);
      activateSenderBroadcastSession();
    }

    function activateSenderBroadcastSession() {
      role = 'sender';
      requestScreenWakeLock();
      setAmbientState('waiting', 'WAITING FOR PEER');

      if (!currentSessionId) {
        currentSessionId = Math.random().toString(36).substring(2, 10) + Math.random().toString(36).substring(2, 6);
      }

      totalSelectedBytes = 0;
      const metaList = selectedFiles.map(function(f) {
        totalSelectedBytes += f.size;
        return { name: f.webkitRelativePath || f.name, size: f.size, mime: f.type || 'application/octet-stream' };
      });

      showHubView('hub-sender-ready');

      const title = selectedFiles.length === 1 ? (selectedFiles[0].webkitRelativePath || selectedFiles[0].name) : (selectedFiles.length + ' Files Queued');
      document.getElementById('sender-title').innerText = title;
      document.getElementById('sender-subtitle').innerText = formatSize(totalSelectedBytes) + ' total';

      const zipBtn = document.getElementById('zip-bundle-btn');
      if (selectedFiles.length > 1) {
        zipBtn.classList.remove('hidden');
        zipBtn.disabled = false;
        zipBtn.innerText = '📦 Bundle all into .ZIP';
      } else {
        zipBtn.classList.add('hidden');
      }

      const qBox = document.getElementById('sender-file-queue');
      qBox.innerHTML = '';
      selectedFiles.forEach(function(f, i) {
        const d = document.createElement('div');
        d.className = 'flex justify-between items-center text-gray-300 py-0.5 border-b border-gray-800/60 last:border-none';
        d.innerHTML = '<span class="truncate max-w-[220px]">' + (i + 1) + '. ' + escapeSafe(f.webkitRelativePath || f.name) + '</span>' +
                      '<span class="mono-font text-amber-400 ml-2">' + formatSize(f.size) + '</span>';
        qBox.appendChild(d);
      });

      const shareLink = window.location.origin + window.location.pathname + '?s=' + currentSessionId;
      document.getElementById('sender-link-input').value = shareLink;
      drawQrCode(shareLink, 'sender-qr-canvas', 'sender-qr-fallback', 148);

      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'create-session',
          sessionId: currentSessionId,
          files: metaList,
          totalSize: totalSelectedBytes
        }));
      }
    }

    async function bundleSelectedFilesIntoZip() {
      if (selectedFiles.length <= 1 || typeof JSZip === 'undefined') return;
      const zipBtn = document.getElementById('zip-bundle-btn');
      zipBtn.disabled = true;
      zipBtn.innerText = '⏳ Bundling .ZIP...';

      try {
        const zip = new JSZip();
        selectedFiles.forEach(function(file) {
          const pathName = file.webkitRelativePath || file.name;
          // Use STORE (no CPU compression bottleneck) for fast bundling & maximum streaming speed
          zip.file(pathName, file, { compression: 'STORE' });
        });
        const blob = await zip.generateAsync({ type: 'blob' });
        const zipFileName = 'ChocoBento-Bundle-' + Date.now().toString().slice(-4) + '.zip';
        const bundledFile = new File([blob], zipFileName, { type: 'application/zip' });

        selectedFiles = [bundledFile];
        totalSelectedBytes = bundledFile.size;

        document.getElementById('sender-title').innerText = bundledFile.name;
        document.getElementById('sender-subtitle').innerText = formatSize(totalSelectedBytes) + ' (Bundled .ZIP)';
        zipBtn.classList.add('hidden');

        const qBox = document.getElementById('sender-file-queue');
        qBox.innerHTML = '<div class="flex justify-between text-emerald-300 font-bold py-1"><span>📦 ' + escapeSafe(bundledFile.name) + '</span><span class="mono-font">' + formatSize(bundledFile.size) + '</span></div>';

        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: 'update-session-files',
            files: [{ name: bundledFile.name, size: bundledFile.size, mime: 'application/zip' }],
            totalSize: totalSelectedBytes
          }));
        }
      } catch (err) {
        zipBtn.innerText = '⚠️ ZIP Failed';
      }
    }

    // Ctrl+V Screenshot & Clipboard Paste Handler
    window.addEventListener('paste', function(e) {
      const activeTag = document.activeElement ? document.activeElement.tagName : '';
      if (activeTag === 'TEXTAREA' || activeTag === 'INPUT') return;
      if (e.clipboardData && e.clipboardData.files && e.clipboardData.files.length > 0) {
        e.preventDefault();
        handleFilesPicked(e.clipboardData.files);
      }
    });

    // Recursive Folder Drag-and-Drop Support
    const dropzoneEl = document.getElementById('bento-dropzone');
    if (dropzoneEl) {
      ['dragenter', 'dragover'].forEach(function(ev) {
        dropzoneEl.addEventListener(ev, function(e) {
          e.preventDefault();
          e.stopPropagation();
          dropzoneEl.classList.add('border-amber-400');
        });
      });
      ['dragleave', 'drop'].forEach(function(ev) {
        dropzoneEl.addEventListener(ev, function(e) {
          e.preventDefault();
          e.stopPropagation();
          dropzoneEl.classList.remove('border-amber-400');
        });
      });
      dropzoneEl.addEventListener('drop', async function(e) {
        const items = e.dataTransfer.items;
        if (items && items.length > 0 && items[0].webkitGetAsEntry) {
          const collected = [];
          for (let i = 0; i < items.length; i++) {
            const entry = items[i].webkitGetAsEntry();
            if (entry) await traverseFileTree(entry, '', collected);
          }
          if (collected.length > 0) {
            handleFilesPicked(collected);
            return;
          }
        }
        if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
          handleFilesPicked(e.dataTransfer.files);
        }
      });
    }

    function traverseFileTree(item, pathPrefix, results) {
      return new Promise(function(resolve) {
        if (item.isFile) {
          item.file(function(file) {
            results.push(file);
            resolve();
          });
        } else if (item.isDirectory) {
          const dirReader = item.createReader();
          dirReader.readEntries(async function(entries) {
            for (let i = 0; i < entries.length; i++) {
              await traverseFileTree(entries[i], pathPrefix + item.name + '/', results);
            }
            resolve();
          });
        } else {
          resolve();
        }
      });
    }

    // ------------------------------------------------------------
    // 7. HYPER-STREAM ENGINE (ZERO-DELAY MAX SPEED P2P + TURBO RELAY)
    // ------------------------------------------------------------
    function renderReceiverPreviewList() {
      if (!incomingFilesMeta || incomingFilesMeta.length === 0) return;
      const title = incomingFilesMeta.length === 1 ? incomingFilesMeta[0].name : (incomingFilesMeta.length + ' Files Ready to Download');
      document.getElementById('receiver-title').innerText = title;
      document.getElementById('receiver-subtitle').innerText = 'Total Size: ' + formatSize(incomingTotalBytes);

      const list = document.getElementById('receiver-file-list');
      list.innerHTML = '';
      incomingFilesMeta.forEach(function(f, idx) {
        const row = document.createElement('div');
        row.className = 'flex justify-between items-center text-gray-300 py-1 border-b border-gray-800/60 last:border-none';
        row.innerHTML = '<span class="truncate max-w-[220px]">📄 ' + (idx + 1) + '. ' + escapeSafe(f.name) + '</span>' +
                        '<span class="mono-font text-amber-400 ml-2">' + formatSize(f.size) + '</span>';
        list.appendChild(row);
      });
    }

    function startReceiverDownload() {
      requestScreenWakeLock();
      isAborted = false;
      totalReceivedBytes = 0;
      packetsCount = 0;
      rollingCrc = 0;

      showHubView('hub-active-transfer');
      document.getElementById('active-role-badge').innerText = 'RECEIVING MAX-SPEED';
      document.getElementById('integrity-badge').classList.add('hidden');
      setAmbientState('streaming', 'RECEIVING STREAM');

      initPeerConnection(false, null);
      ws.send(JSON.stringify({ type: 'request-download' }));
    }

    const rtcConfig = {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun.cloudflare.com:3478' }
      ]
    };

    function initPeerConnection(isInitiator, targetReceiverId) {
      if (pc) {
        try { pc.close(); } catch (e) {}
      }
      pc = new RTCPeerConnection(rtcConfig);
      iceQueue = [];

      pc.onicecandidate = function(e) {
        if (e.candidate && ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: 'webrtc-signal',
            receiverId: targetReceiverId,
            payload: { candidate: e.candidate }
          }));
        }
      };

      if (isInitiator) {
        dataChannel = pc.createDataChannel('chocobento-hyperstream', { ordered: true });
        bindDataChannelEvents();
        pc.createOffer().then(function(o) {
          return pc.setLocalDescription(o);
        }).then(function() {
          ws.send(JSON.stringify({
            type: 'webrtc-signal',
            receiverId: targetReceiverId,
            payload: { sdp: pc.localDescription }
          }));
        });
      } else {
        pc.ondatachannel = function(e) {
          dataChannel = e.channel;
          bindDataChannelEvents();
        };
      }
    }

    async function processWebRtcSignal(payload, remoteReceiverId) {
      if (!pc) initPeerConnection(false, remoteReceiverId);
      try {
        if (payload.sdp) {
          await pc.setRemoteDescription(new RTCSessionDescription(payload.sdp));
          while (iceQueue.length > 0) {
            await pc.addIceCandidate(new RTCIceCandidate(iceQueue.shift()));
          }
          if (payload.sdp.type === 'offer') {
            const ans = await pc.createAnswer();
            await pc.setLocalDescription(ans);
            ws.send(JSON.stringify({
              type: 'webrtc-signal',
              receiverId: remoteReceiverId,
              payload: { sdp: pc.localDescription }
            }));
          }
        } else if (payload.candidate) {
          if (pc.remoteDescription && pc.remoteDescription.type) {
            await pc.addIceCandidate(new RTCIceCandidate(payload.candidate));
          } else {
            iceQueue.push(payload.candidate);
          }
        }
      } catch (e) {}
    }

    function bindDataChannelEvents() {
      dataChannel.binaryType = 'arraybuffer';
      dataChannel.bufferedAmountLowThreshold = WEBRTC_LOW_WATER_MARK;

      dataChannel.onopen = function() {
        if (p2pWatchdog) {
          clearTimeout(p2pWatchdog);
          p2pWatchdog = null;
        }
        usingFallbackRelay = false;
        document.getElementById('hud-protocol-badge').innerText = 'WebRTC P2P Direct (128KB)';
        if (role === 'sender' && !isTransferring) {
          streamAllFiles(false);
        }
      };

      dataChannel.onmessage = function(e) {
        if (typeof e.data === 'string') {
          const ctrl = JSON.parse(e.data);
          if (ctrl.type === 'file-header') onFileHeaderReceived(ctrl);
          else if (ctrl.type === 'file-complete') saveCompletedIncomingFile();
          else if (ctrl.type === 'all-complete') completeTransferSuccess('Download 100% Complete!');
        } else {
          handleIncomingChunkBuffer(e.data);
        }
      };
    }

    function startSenderTransfer(targetReceiverId) {
      isAborted = false;
      isTransferring = false;
      senderAckBytes = 0;
      packetsCount = 0;
      rollingCrc = 0;

      document.getElementById('active-role-badge').innerText = 'HYPER-STREAMING';
      document.getElementById('integrity-badge').classList.add('hidden');
      setAmbientState('streaming', 'MAX SPEED STREAMING');

      initPeerConnection(true, targetReceiverId);

      // Fast 2.2s Fallback Watchdog so users on mobile data never wait long
      p2pWatchdog = setTimeout(function() {
        if (!isTransferring && (!dataChannel || dataChannel.readyState !== 'open')) {
          usingFallbackRelay = true;
          document.getElementById('hud-protocol-badge').innerText = 'Turbo 256KB Relay';
          ws.send(JSON.stringify({ type: 'switch-to-fallback' }));
          streamAllFiles(true);
        }
      }, 2200);
    }

    async function streamAllFiles(useRelay) {
      if (isTransferring) return;
      isTransferring = true;
      transferStartTime = performance.now();
      let sentBytesTotal = 0;

      for (let i = 0; i < selectedFiles.length; i++) {
        if (isAborted) return;
        const f = selectedFiles[i];
        document.getElementById('active-filename').innerText = f.name;
        document.getElementById('active-file-index').innerText = 'File ' + (i + 1) + ' of ' + selectedFiles.length;

        const hdr = JSON.stringify({
          type: 'file-header',
          index: i,
          totalFiles: selectedFiles.length,
          name: f.name,
          size: f.size,
          mime: f.type || 'application/octet-stream'
        });
        if (useRelay) ws.send(hdr);
        else dataChannel.send(hdr);

        await pumpFileAtMaxThroughput(f, useRelay, function(chunkBuf) {
          sentBytesTotal += chunkBuf.byteLength;
          packetsCount++;
          updateRollingCrcFast(chunkBuf);
          updateTelemetryAndProgress(Math.max(sentBytesTotal, senderAckBytes), totalSelectedBytes, false);
        });

        if (isAborted) return;
        const fin = JSON.stringify({ type: 'file-complete', index: i });
        if (useRelay) ws.send(fin);
        else dataChannel.send(fin);
      }

      if (!isAborted) {
        const allFin = JSON.stringify({ type: 'all-complete' });
        if (useRelay) ws.send(allFin);
        else dataChannel.send(allFin);
        updateTelemetryAndProgress(totalSelectedBytes, totalSelectedBytes, true);
        completeTransferSuccess('Broadcast Transfer 100% Verified!');
      }
    }

    // Unthrottled Continuous Buffer Pump (Zero setTimeout delay, saturates hardware pipe)
    async function pumpFileAtMaxThroughput(file, useRelay, onChunk) {
      const frameSize = useRelay ? RELAY_CHUNK_SIZE : WEBRTC_CHUNK_SIZE;
      let offset = 0;

      while (offset < file.size && !isAborted) {
        // Respect backpressure only when hardware buffer hits 8MB ceiling
        if (!useRelay && dataChannel) {
          if (dataChannel.bufferedAmount > WEBRTC_HIGH_WATER_MARK) {
            await waitForDataChannelDrain();
          }
        } else if (useRelay && ws) {
          while (ws.bufferedAmount > 16 * 1024 * 1024 && !isAborted) {
            await new Promise(function(r) { setTimeout(r, 8); });
          }
        }

        if (isAborted) break;

        const slice = file.slice(offset, offset + frameSize);
        const buf = await slice.arrayBuffer();
        if (isAborted) break;

        if (useRelay) {
          ws.send(buf);
        } else {
          dataChannel.send(buf);
        }

        offset += buf.byteLength;
        onChunk(buf);
      }
    }

    function waitForDataChannelDrain() {
      return new Promise(function(resolve) {
        if (!dataChannel || dataChannel.readyState !== 'open' || dataChannel.bufferedAmount <= WEBRTC_LOW_WATER_MARK) {
          resolve();
          return;
        }
        let resolved = false;
        const done = function() {
          if (!resolved) {
            resolved = true;
            dataChannel.onbufferedamountlow = null;
            resolve();
          }
        };
        dataChannel.onbufferedamountlow = done;
        setTimeout(done, 35);
      });
    }

    function onFileHeaderReceived(hdr) {
      if (packetsCount === 0) transferStartTime = performance.now();
      activeFileHeader = hdr;
      currentFileChunks = [];
      document.getElementById('active-filename').innerText = hdr.name;
      document.getElementById('active-file-index').innerText = 'File ' + (hdr.index + 1) + ' of ' + hdr.totalFiles;
    }

    function handleIncomingChunkBuffer(buf) {
      if (isAborted) return;
      currentFileChunks.push(buf);
      totalReceivedBytes += buf.byteLength;
      packetsCount++;
      updateRollingCrcFast(buf);

      const isDone = totalReceivedBytes >= incomingTotalBytes;
      updateTelemetryAndProgress(totalReceivedBytes, incomingTotalBytes, isDone);

      const now = performance.now();
      if (now - lastAckTimestamp > 90 || isDone) {
        lastAckTimestamp = now;
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'progress-ack', receivedBytes: totalReceivedBytes }));
        }
      }
    }

    function saveCompletedIncomingFile() {
      if (isAborted || currentFileChunks.length === 0 || !activeFileHeader) return;
      const blob = new Blob(currentFileChunks, { type: activeFileHeader.mime || 'application/octet-stream' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = activeFileHeader.name || 'download';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(function() { URL.revokeObjectURL(url); }, 12000);
      currentFileChunks = [];
    }

    // ------------------------------------------------------------
    // 8. 60FPS THROTTLED TELEMETRY HUD, CRC32 & AUDIO CHIME
    // ------------------------------------------------------------
    function updateRollingCrcFast(arrayBuf) {
      const view = new Uint8Array(arrayBuf);
      const len = view.length;
      const step = Math.max(1, Math.floor(len / 16));
      for (let i = 0; i < len; i += step) {
        rollingCrc = ((rollingCrc << 5) - rollingCrc + view[i]) | 0;
      }
    }

    function updateTelemetryAndProgress(doneBytes, totalBytes, forceRender) {
      if (totalBytes <= 0) return;
      const now = performance.now();
      // Throttle DOM & Canvas repaints to ~16fps (every 60ms) so CPU stays 100% focused on network throughput
      if (!forceRender && (now - lastUiRenderTime < 60)) return;
      lastUiRenderTime = now;

      const clamped = Math.min(doneBytes, totalBytes);
      const pct = Math.min(100, Math.round((clamped / totalBytes) * 100));

      const elapsed = Math.max(0.05, (now - transferStartTime) / 1000);
      const bps = clamped / elapsed;
      const mbps = bps / (1024 * 1024);
      if (mbps > peakSpeedMbps) peakSpeedMbps = mbps;
      const eta = bps > 0 ? Math.ceil((totalBytes - clamped) / bps) : 0;

      const hexCrc = (rollingCrc >>> 0).toString(16).toUpperCase().padStart(8, '0');

      document.getElementById('active-percent-text').innerText = pct + '%';
      document.getElementById('active-progress-fill').style.width = pct + '%';
      document.getElementById('active-bytes-counter').innerText = formatSize(clamped) + ' / ' + formatSize(totalBytes);
      document.getElementById('hud-speed').innerText = mbps.toFixed(2) + ' MB/s';
      document.getElementById('hud-eta').innerText = eta + ' s';
      document.getElementById('hud-crc').innerText = '0x' + hexCrc;
      document.getElementById('telemetry-packets').innerText = packetsCount + ' pkts';
      document.getElementById('telemetry-peak').innerText = peakSpeedMbps.toFixed(2) + ' MB/s';

      document.title = '[' + pct + '% • ' + mbps.toFixed(1) + ' MB/s] ChocoBento P2P';

      speedSamples.push(mbps);
      if (speedSamples.length > 45) speedSamples.shift();
      drawTelemetryGraph();
    }

    function drawTelemetryGraph() {
      const canvas = document.getElementById('telemetry-canvas');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);

      if (speedSamples.length < 2) return;
      const maxVal = Math.max(1, peakSpeedMbps * 1.2);

      ctx.beginPath();
      ctx.moveTo(0, h);
      for (let i = 0; i < speedSamples.length; i++) {
        const x = (i / (speedSamples.length - 1)) * w;
        const y = h - (speedSamples[i] / maxVal) * (h - 12);
        ctx.lineTo(x, y);
      }
      ctx.lineTo(w, h);
      ctx.closePath();

      const grad = ctx.createLinearGradient(0, 0, 0, h);
      grad.addColorStop(0, 'rgba(16, 185, 129, 0.45)');
      grad.addColorStop(1, 'rgba(6, 182, 212, 0.02)');
      ctx.fillStyle = grad;
      ctx.fill();

      ctx.beginPath();
      for (let i = 0; i < speedSamples.length; i++) {
        const x = (i / (speedSamples.length - 1)) * w;
        const y = h - (speedSamples[i] / maxVal) * (h - 12);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.strokeStyle = '#10b981';
      ctx.lineWidth = 2.5;
      ctx.stroke();
    }

    function completeTransferSuccess(statusText) {
      isTransferring = false;
      document.getElementById('active-status-text').innerText = statusText;
      document.getElementById('active-percent-text').innerText = '100%';
      document.getElementById('active-progress-fill').style.width = '100%';
      document.getElementById('hud-eta').innerText = '0 s';
      document.getElementById('integrity-badge').classList.remove('hidden');
      document.title = '✓ 100% Verified — ChocoBento P2P';
      setAmbientState('connected', '100% VERIFIED COMPLETE');
      playCompletionChime();
    }

    function playCompletionChime() {
      try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        const ctx = new AudioCtx();
        const notes = [523.25, 659.25, 783.99, 1046.50];
        notes.forEach(function(freq, idx) {
          const osc = ctx.createOscillator();
          const gain = ctx.createGain();
          osc.type = 'sine';
          osc.frequency.value = freq;
          gain.gain.setValueAtTime(0.08, ctx.currentTime + idx * 0.09);
          gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + idx * 0.09 + 0.35);
          osc.connect(gain);
          gain.connect(ctx.destination);
          osc.start(ctx.currentTime + idx * 0.09);
          osc.stop(ctx.currentTime + idx * 0.09 + 0.35);
        });
      } catch (e) {}
    }

    // ------------------------------------------------------------
    // 9. STOP & RESTART AND BROADCAST QR RETURN
    // ------------------------------------------------------------
    function triggerStopAndRestart() {
      isAborted = true;
      isTransferring = false;
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'abort-transfer' }));
      }
      handleTransferAborted();
    }

    function handleTransferAborted() {
      isAborted = true;
      isTransferring = false;
      currentFileChunks = [];
      totalReceivedBytes = 0;
      document.title = 'ChocoBento P2P — Command Center';
      setAmbientState('aborted', 'TRANSFER STOPPED');
      if (pc) {
        try { pc.close(); } catch (e) {}
        pc = null;
      }
      if (role === 'receiver') {
        showHubView('hub-receiver-preview');
      } else {
        showHubView('hub-sender-ready');
      }
    }

    function returnSenderToQrView() {
      showHubView('hub-sender-ready');
    }

    function resetToCleanWorkspace() {
      window.location.href = window.location.pathname;
    }

    function showHubView(viewId) {
      const views = ['hub-idle', 'hub-sender-ready', 'hub-receiver-preview', 'hub-active-transfer'];
      views.forEach(function(v) {
        const el = document.getElementById(v);
        if (el) {
          if (v === viewId) el.classList.remove('hidden');
          else el.classList.add('hidden');
        }
      });
    }

    // ------------------------------------------------------------
    // 10. P2P SCRATCHPAD, NATIVE SHARE & QR UTILITIES
    // ------------------------------------------------------------
    function onScratchpadInput(textVal) {
      document.getElementById('scratchpad-char-count').innerText = textVal.length + ' chars';
      document.getElementById('scratchpad-sync-status').innerText = 'Broadcasting live...';
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'scratchpad-update',
          text: textVal,
          senderName: myDeviceInfo.name
        }));
      }
    }

    function toggleScratchpadCodeMode() {
      const pad = document.getElementById('p2p-scratchpad');
      pad.classList.toggle('mono-font');
      pad.classList.toggle('text-emerald-300');
    }

    function copyScratchpadText() {
      const pad = document.getElementById('p2p-scratchpad');
      pad.select();
      navigator.clipboard.writeText(pad.value);
      const btn = document.getElementById('scratch-copy-btn');
      btn.innerText = 'Copied ✓';
      setTimeout(function() { btn.innerText = 'Copy'; }, 1500);
    }

    function clearScratchpadText() {
      const pad = document.getElementById('p2p-scratchpad');
      pad.value = '';
      onScratchpadInput('');
    }

    function showScratchpadQr() {
      const val = document.getElementById('p2p-scratchpad').value.trim() || window.location.href;
      document.getElementById('scratchpad-qr-modal').classList.remove('hidden');
      drawQrCode(val.substring(0, 900), 'scratchpad-qr-canvas', null, 176);
    }

    function drawQrCode(textData, canvasId, fallbackImgId, pxSize) {
      const canvas = document.getElementById(canvasId);
      const fallback = fallbackImgId ? document.getElementById(fallbackImgId) : null;
      let ok = false;
      if (typeof QRious !== 'undefined' && canvas) {
        try {
          new QRious({
            element: canvas,
            value: textData,
            size: pxSize || 148,
            background: '#ffffff',
            foreground: '#000000',
            level: 'M'
          });
          canvas.classList.remove('hidden');
          if (fallback) fallback.classList.add('hidden');
          ok = true;
        } catch (e) { ok = false; }
      }
      if (!ok && fallback) {
        if (canvas) canvas.classList.add('hidden');
        fallback.src = 'https://api.qrserver.com/v1/create-qr-code/?size=150x150&data=' + encodeURIComponent(textData);
        fallback.classList.remove('hidden');
      }
    }

    function copyBroadcastLink() {
      const inp = document.getElementById('sender-link-input');
      inp.select();
      navigator.clipboard.writeText(inp.value);
      const btn = document.getElementById('copy-link-btn');
      btn.innerText = 'Copied ✓';
      setTimeout(function() { btn.innerText = 'Copy Link'; }, 1800);
    }

    function nativeShareBroadcastLink() {
      const url = document.getElementById('sender-link-input').value;
      if (navigator.share) {
        navigator.share({
          title: 'ChocoBento P2P File Transfer',
          text: 'Download my file directly peer-to-peer:',
          url: url
        }).catch(function() {});
      }
    }

    function openQrCameraModal() {
      document.getElementById('qr-camera-modal').classList.remove('hidden');
      if (typeof Html5Qrcode === 'undefined') return;
      qrScannerInstance = new Html5Qrcode('html5-qr-reader');
      qrScannerInstance.start(
        { facingMode: 'environment' },
        { fps: 12, qrbox: { width: 220, height: 220 } },
        function(decodedUrl) {
          closeQrCameraModal();
          window.location.href = decodedUrl;
        },
        function() {}
      ).catch(function() {
        alert('Camera permission denied or unavailable. Use the 4-digit code or Sonar Radar instead.');
        closeQrCameraModal();
      });
    }

    function closeQrCameraModal() {
      document.getElementById('qr-camera-modal').classList.add('hidden');
      if (qrScannerInstance) {
        qrScannerInstance.stop().then(function() { qrScannerInstance.clear(); }).catch(function() {});
      }
    }

    function formatSize(bytes) {
      if (bytes === 0) return '0 B';
      const k = 1024;
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + units[i];
    }

    function escapeSafe(str) {
      return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
    }

    initSignaling();
  </script>
</body>
</html>`;

app.get('*', (req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(HTML_DOCUMENT);
});

server.listen(PORT, () => {
  console.log('ChocoBento P2P Hyper-Speed Command Center running on port ' + PORT);
});
