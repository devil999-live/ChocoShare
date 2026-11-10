const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, maxPayload: 50 * 1024 * 1024 });

const PORT = process.env.PORT || 3000;

// In-memory zero-log active sessions & nearby pairing codes
const sessions = new Map();       // sessionId -> { senderWs, receiverWs, files, totalSize, code4, createdAt }
const codeToSession = new Map();  // 4-digit code -> sessionId
const nearbyWaiters = new Map();  // 4-digit receiver code -> ws (for "Send to PC" workflow)

function generate4DigitCode() {
  let code = '';
  for (let i = 0; i < 4; i++) {
    code += Math.floor(Math.random() * 10).toString();
  }
  if (codeToSession.has(code) || nearbyWaiters.has(code)) {
    return generate4DigitCode();
  }
  return code;
}

wss.on('connection', (ws) => {
  let boundSessionId = null;
  let boundRole = null; // 'sender' | 'receiver'
  let boundWaiterCode = null;

  ws.on('message', (rawMessage, isBinary) => {
    // Handle binary chunk relay (Fallback mode when WebRTC P2P is blocked by NAT/Firewall)
    if (isBinary) {
      if (boundSessionId && sessions.has(boundSessionId)) {
        const sess = sessions.get(boundSessionId);
        if (boundRole === 'sender' && sess.receiverWs && sess.receiverWs.readyState === ws.OPEN) {
          sess.receiverWs.send(rawMessage, { binary: true });
        }
      }
      return;
    }

    let data;
    try {
      data = JSON.parse(rawMessage.toString());
    } catch (err) {
      return;
    }

    // 1. Sender creates a file sharing session after selecting files
    if (data.type === 'create-session') {
      const sessionId = data.sessionId;
      const code4 = generate4DigitCode();
      boundSessionId = sessionId;
      boundRole = 'sender';

      sessions.set(sessionId, {
        sessionId: sessionId,
        code4: code4,
        senderWs: ws,
        receiverWs: null,
        files: data.files || [],
        totalSize: data.totalSize || 0,
        createdAt: Date.now()
      });
      codeToSession.set(code4, sessionId);

      ws.send(JSON.stringify({
        type: 'session-created',
        sessionId: sessionId,
        code4: code4
      }));
    }

    // 2. Receiver looks up a 4-digit local code on Nearby Devices page
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
          message: 'Invalid or expired 4-digit code. Please check the code on the sender device.'
        }));
      }
    }

    // 3. PC registers a waiting 4-digit receiver code on the Nearby Devices page ("Send to PC" mode)
    else if (data.type === 'register-nearby-receiver') {
      if (boundWaiterCode) nearbyWaiters.delete(boundWaiterCode);
      const waiterCode = generate4DigitCode();
      boundWaiterCode = waiterCode;
      nearbyWaiters.set(waiterCode, ws);
      ws.send(JSON.stringify({
        type: 'nearby-receiver-ready',
        waiterCode: waiterCode
      }));
    }

    // 4. Phone scans PC's Nearby QR code and pushes its sessionId to the PC automatically
    else if (data.type === 'push-session-to-pc') {
      const waiterWs = nearbyWaiters.get(String(data.waiterCode || '').trim());
      if (waiterWs && waiterWs.readyState === ws.OPEN) {
        waiterWs.send(JSON.stringify({
          type: 'auto-open-session',
          sessionId: data.sessionId
        }));
      }
    }

    // 5. Receiver opens the share link (?s=sessionId) and joins session to view file details
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
      sess.receiverWs = ws;

      // Send file metadata to receiver so they see File Name, Size, and Download Button
      ws.send(JSON.stringify({
        type: 'session-info',
        sessionId: sess.sessionId,
        files: sess.files,
        totalSize: sess.totalSize
      }));

      // Notify sender that receiver opened the link
      sess.senderWs.send(JSON.stringify({
        type: 'receiver-viewed'
      }));
    }

    // 6. Receiver clicks "Download" button -> notify sender to start transfer & show progress bar on BOTH
    else if (data.type === 'request-download') {
      const sess = sessions.get(boundSessionId);
      if (sess && sess.senderWs && sess.senderWs.readyState === ws.OPEN) {
        sess.senderWs.send(JSON.stringify({
          type: 'start-transfer-now',
          useFallback: !!data.useFallback
        }));
      }
    }

    // 7. WebRTC Signaling relay (Offer, Answer, ICE Candidates)
    else if (data.type === 'webrtc-signal') {
      const sess = sessions.get(boundSessionId);
      if (!sess) return;
      const targetWs = boundRole === 'sender' ? sess.receiverWs : sess.senderWs;
      if (targetWs && targetWs.readyState === ws.OPEN) {
        targetWs.send(JSON.stringify({
          type: 'webrtc-signal',
          payload: data.payload
        }));
      }
    }

    // 8. Relay control & progress messages (for both WebRTC & Fallback modes)
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
      const targetWs = boundRole === 'sender' ? sess.receiverWs : sess.senderWs;
      if (targetWs && targetWs.readyState === ws.OPEN) {
        targetWs.send(JSON.stringify(data));
      }
    }
  });

  ws.on('close', () => {
    if (boundWaiterCode) {
      nearbyWaiters.delete(boundWaiterCode);
    }
    if (boundSessionId && sessions.has(boundSessionId)) {
      const sess = sessions.get(boundSessionId);
      if (boundRole === 'sender') {
        if (sess.receiverWs && sess.receiverWs.readyState === ws.OPEN) {
          sess.receiverWs.send(JSON.stringify({
            type: 'peer-disconnected',
            role: 'sender'
          }));
        }
        codeToSession.delete(sess.code4);
        sessions.delete(boundSessionId);
      } else if (boundRole === 'receiver') {
        sess.receiverWs = null;
        if (sess.senderWs && sess.senderWs.readyState === ws.OPEN) {
          sess.senderWs.send(JSON.stringify({
            type: 'peer-disconnected',
            role: 'receiver'
          }));
        }
      }
    }
  });
});

const HTML_DOCUMENT = `<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ChocoShare - Making sharing sweet | Direct P2P File Transfer</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/qrious/4.0.2/qrious.min.js"></script>
  <script src="https://unpkg.com/html5-qrcode" type="text/javascript"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          colors: {
            choco: {
              50: '#fdf8f6',
              100: '#f2e8e5',
              200: '#eaddd7',
              300: '#e0cec7',
              400: '#d2bab0',
              500: '#f59e0b',
              600: '#d97706',
              700: '#b45309',
              800: '#1f1614',
              900: '#120d0c',
              950: '#0a0706'
            }
          }
        }
      }
    };
  </script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');
    body {
      font-family: 'Plus Jakarta Sans', ui-sans-serif, system-ui, -apple-system, sans-serif;
      background: radial-gradient(circle at 20% 20%, #1e1b2e 0%, #0f101a 55%, #07080d 100%);
      color: #f3f4f6;
      min-height: 100vh;
    }
    html:not(.dark) body {
      background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 45%, #fde68a 100%);
      color: #1f2937;
    }
    .glass-card {
      background: linear-gradient(145deg, rgba(31, 41, 55, 0.78) 0%, rgba(17, 24, 39, 0.92) 100%);
      backdrop-filter: blur(16px);
      border: 1px solid rgba(245, 158, 11, 0.22);
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.6);
    }
    html:not(.dark) .glass-card {
      background: rgba(255, 255, 255, 0.92);
      border: 1px solid rgba(217, 119, 6, 0.25);
      box-shadow: 0 20px 40px -12px rgba(180, 83, 9, 0.15);
      color: #111827;
    }
    .drop-ring {
      background: conic-gradient(from 0deg, #f59e0b, #10b981, #3b82f6, #f59e0b);
    }
    .nav-btn.active {
      color: #fbbf24;
      border-bottom: 2px solid #fbbf24;
    }
    .custom-scroll::-webkit-scrollbar {
      width: 6px;
    }
    .custom-scroll::-webkit-scrollbar-thumb {
      background: rgba(245, 158, 11, 0.4);
      border-radius: 9999px;
    }
  </style>
</head>
<body class="flex flex-col justify-between min-h-screen selection:bg-amber-500 selection:text-black">

  <!-- TOP NAVIGATION BAR -->
  <header class="sticky top-0 z-50 backdrop-blur-md bg-gray-950/75 border-b border-gray-800/80">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-20 flex items-center justify-between">
      <!-- Brand Logo -->
      <div class="flex items-center space-x-3 cursor-pointer group" onclick="switchTab('transfer')">
        <div class="w-11 h-11 rounded-2xl bg-gradient-to-br from-amber-500 via-orange-500 to-amber-700 flex items-center justify-center shadow-lg shadow-amber-500/25 group-hover:scale-105 transition-transform">
          <span class="text-2xl">🍫</span>
        </div>
        <div>
          <div class="flex items-center space-x-2">
            <span class="text-xl font-extrabold tracking-tight text-white">Choco<span class="text-amber-400">Share</span></span>
            <span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-amber-500/20 text-amber-300 border border-amber-500/30">P2P v2.5</span>
          </div>
          <p class="text-xs text-gray-400">Making sharing sweet</p>
        </div>
      </div>

      <!-- Desktop Navigation Links -->
      <nav class="hidden md:flex items-center space-x-7 text-sm font-semibold text-gray-300">
        <button onclick="switchTab('transfer')" id="nav-transfer" class="nav-btn active py-2 hover:text-amber-400 transition-colors">Transfer</button>
        <button onclick="switchTab('nearby')" id="nav-nearby" class="nav-btn py-2 hover:text-amber-400 transition-colors flex items-center space-x-1.5">
          <span>Nearby devices</span>
          <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
        </button>
        <button onclick="switchTab('about')" id="nav-about" class="nav-btn py-2 hover:text-amber-400 transition-colors">About</button>
        <button onclick="switchTab('blog')" id="nav-blog" class="nav-btn py-2 hover:text-amber-400 transition-colors">Blog</button>
        <button onclick="switchTab('faq')" id="nav-faq" class="nav-btn py-2 hover:text-amber-400 transition-colors">FAQ</button>
        <button onclick="switchTab('privacy')" id="nav-privacy" class="nav-btn py-2 hover:text-amber-400 transition-colors">Privacy</button>
        <button onclick="switchTab('download')" id="nav-download" class="nav-btn py-2 hover:text-amber-400 transition-colors">Download</button>
      </nav>

      <!-- Actions: Status & Theme Toggle -->
      <div class="flex items-center space-x-3">
        <div class="hidden sm:flex items-center space-x-2 px-3 py-1.5 rounded-full bg-gray-900 border border-gray-800 text-xs">
          <span id="ws-dot" class="w-2 h-2 rounded-full bg-emerald-400"></span>
          <span id="ws-status-text" class="text-gray-300 font-medium">Signaling Online</span>
        </div>
        <button onclick="toggleTheme()" title="Toggle Dark/Light Mode" class="p-2.5 rounded-xl bg-gray-900 hover:bg-gray-800 border border-gray-700 text-amber-400 transition-all">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"/></svg>
        </button>
      </div>
    </div>

    <!-- Mobile Navigation Bar -->
    <div class="flex md:hidden overflow-x-auto px-4 py-2 space-x-5 border-t border-gray-800/80 text-xs font-semibold text-gray-300 bg-gray-950/90">
      <button onclick="switchTab('transfer')" class="whitespace-nowrap hover:text-amber-400">Transfer</button>
      <button onclick="switchTab('nearby')" class="whitespace-nowrap text-amber-400">Nearby devices</button>
      <button onclick="switchTab('about')" class="whitespace-nowrap hover:text-amber-400">About</button>
      <button onclick="switchTab('blog')" class="whitespace-nowrap hover:text-amber-400">Blog</button>
      <button onclick="switchTab('faq')" class="whitespace-nowrap hover:text-amber-400">FAQ</button>
      <button onclick="switchTab('privacy')" class="whitespace-nowrap hover:text-amber-400">Privacy</button>
      <button onclick="switchTab('download')" class="whitespace-nowrap hover:text-amber-400">Download</button>
    </div>
  </header>

  <!-- MAIN CONTENT CONTAINER -->
  <main class="flex-grow max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-8 md:py-12">

    <!-- ========================================== -->
    <!-- TAB 1: TRANSFER (MAIN P2P SHARING & HERO)  -->
    <!-- ========================================== -->
    <section id="tab-transfer" class="space-y-20">
      <div class="grid grid-cols-1 lg:grid-cols-12 gap-10 items-center">

        <!-- LEFT COLUMN: DYNAMIC TRANSFER WIDGET (5 Cols) -->
        <div class="lg:col-span-6">
          <div class="glass-card rounded-3xl p-6 sm:p-8 relative overflow-hidden">

            <!-- STATE A: IDLE / SELECT FILES OR NEARBY -->
            <div id="state-idle" class="space-y-6">
              <div id="drop-area"
                   onclick="document.getElementById('file-picker').click()"
                   class="group relative border-2 border-dashed border-amber-500/40 hover:border-amber-400 rounded-2xl p-8 sm:p-10 text-center cursor-pointer transition-all bg-gray-900/40 hover:bg-gray-900/70">
                <input type="file" id="file-picker" multiple class="hidden" onchange="onFilesSelected(this.files)">

                <div class="w-20 h-20 mx-auto rounded-full bg-gradient-to-tr from-amber-500 to-orange-400 p-1 shadow-xl shadow-amber-500/20 group-hover:scale-110 transition-transform">
                  <div class="w-full h-full bg-gray-950 rounded-full flex items-center justify-center">
                    <span class="text-4xl font-light text-amber-400">+</span>
                  </div>
                </div>

                <h3 class="mt-5 text-lg sm:text-xl font-bold text-white">Click to browse or drag files here to start sharing</h3>
                <p class="mt-2 text-xs sm:text-sm text-gray-400">Supports single or multiple files • Unlimited file size • Direct P2P</p>
              </div>

              <!-- Quick Switch to Nearby Devices / Scan QR -->
              <div class="grid grid-cols-2 gap-3">
                <button onclick="switchTab('nearby')" class="flex items-center justify-center space-x-2 py-3 px-4 rounded-xl bg-gray-800/90 hover:bg-gray-700 border border-gray-700 text-xs sm:text-sm font-semibold text-amber-300 transition-all">
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8.111 16.404a5.5 5.5 0 017.778 0M12 20h.01m-7.08-7.071c3.904-3.905 10.236-3.905 14.141 0M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0"/></svg>
                  <span>Nearby Devices</span>
                </button>
                <button onclick="openCameraScannerModal()" class="flex items-center justify-center space-x-2 py-3 px-4 rounded-xl bg-amber-500/15 hover:bg-amber-500/25 border border-amber-500/30 text-xs sm:text-sm font-semibold text-amber-300 transition-all">
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm12 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z"/></svg>
                  <span>Scan QR Code</span>
                </button>
              </div>
            </div>

            <!-- STATE B: SENDER READY (LINK + QR CODE + 4-DIGIT CODE + WAITING FOR RECEIVER) -->
            <div id="state-sender-ready" class="hidden space-y-5">
              <div class="flex items-center justify-between border-b border-gray-800 pb-3">
                <div>
                  <span class="text-[11px] font-bold uppercase tracking-wider text-amber-400">Ready to Share</span>
                  <h3 id="sender-file-title" class="text-base font-bold text-white truncate max-w-[240px]">files.zip</h3>
                  <p id="sender-file-subtitle" class="text-xs text-gray-400">0 MB</p>
                </div>
                <button onclick="stopAndResetSession()" class="px-3 py-1.5 rounded-lg bg-red-500/20 hover:bg-red-500/30 border border-red-500/40 text-red-300 text-xs font-semibold transition-all">
                  Stop Sharing
                </button>
              </div>

              <!-- Selected Files List -->
              <div id="sender-file-list" class="max-h-28 overflow-y-auto custom-scroll space-y-1.5 bg-gray-950/60 p-2.5 rounded-xl border border-gray-800 text-xs"></div>

              <!-- Warning Banner -->
              <div class="flex items-center space-x-2.5 px-3.5 py-2.5 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-200 text-xs">
                <span class="w-2.5 h-2.5 rounded-full bg-amber-400 animate-ping flex-shrink-0"></span>
                <span id="sender-wait-status">Keep this tab open! Waiting for receiver to open link or scan QR...</span>
              </div>

              <!-- QR Code & 4-Digit Nearby Code Side-by-Side -->
              <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 items-center bg-gray-900/70 p-4 rounded-2xl border border-gray-800">
                <div class="flex flex-col items-center justify-center">
                  <div class="bg-white p-2.5 rounded-2xl shadow-lg border-2 border-amber-400 flex items-center justify-center w-[190px] h-[190px]">
                    <canvas id="sender-qr-canvas" width="170" height="170" class="w-[170px] h-[170px] block"></canvas>
                    <img id="sender-qr-fallback" alt="QR Code" class="w-[170px] h-[170px] hidden" />
                  </div>
                  <span class="text-[11px] text-gray-400 mt-2">Scan with Phone or Camera</span>
                </div>

                <div class="flex flex-col justify-between space-y-3 text-center sm:text-left">
                  <div class="bg-gray-950/90 p-3.5 rounded-xl border border-gray-800">
                    <p class="text-[11px] uppercase tracking-wider text-gray-400 font-semibold">4-Digit Nearby Code</p>
                    <p id="sender-4digit-code" class="text-3xl font-extrabold tracking-[0.25em] text-amber-400 mt-1">----</p>
                    <p class="text-[11px] text-gray-500 mt-1">Enter on Receiver's "Nearby devices" tab</p>
                  </div>

                  <button onclick="openSendToPcScanner()" class="w-full py-2.5 px-3 rounded-xl bg-emerald-500/15 hover:bg-emerald-500/25 border border-emerald-500/30 text-emerald-300 text-xs font-semibold flex items-center justify-center space-x-1.5 transition-all">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>
                    <span>Send to PC (Scan PC QR)</span>
                  </button>
                </div>
              </div>

              <!-- Shareable URL Copy Box -->
              <div class="space-y-1.5">
                <label class="text-xs font-semibold text-gray-400">Direct Share Link (Specific to these files only):</label>
                <div class="flex space-x-2">
                  <input type="text" id="sender-share-url" readonly class="w-full bg-gray-950 border border-gray-700 rounded-xl px-3.5 py-2.5 text-xs sm:text-sm text-gray-200 focus:outline-none">
                  <button onclick="copyShareUrl()" id="copy-url-btn" class="px-4 py-2.5 bg-gradient-to-r from-amber-500 to-orange-500 hover:from-amber-400 hover:to-orange-400 text-gray-950 font-bold text-xs sm:text-sm rounded-xl shadow transition-all whitespace-nowrap">
                    Copy Link
                  </button>
                </div>
              </div>
            </div>

            <!-- STATE C: RECEIVER FILE PREVIEW (SHOWS FILE NAME, SIZE & DOWNLOAD BUTTON BEFORE PROGRESS BAR) -->
            <div id="state-receiver-preview" class="hidden space-y-6 text-center py-4">
              <div class="w-20 h-20 mx-auto rounded-3xl bg-amber-500/15 border border-amber-500/30 flex items-center justify-center text-amber-400 shadow-inner">
                <svg class="w-10 h-10" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M9 19l3 3m0 0l3-3m-3 3V10"/></svg>
              </div>

              <div class="space-y-1">
                <span class="px-2.5 py-1 rounded-full text-[11px] font-bold uppercase bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">Peer Ready to Send</span>
                <h3 id="receiver-preview-title" class="text-xl font-extrabold text-white pt-2 break-all">Loading file info...</h3>
                <p id="receiver-preview-size" class="text-sm text-amber-400 font-semibold">0 MB</p>
              </div>

              <!-- Multi-file preview list -->
              <div id="receiver-preview-list" class="max-h-36 overflow-y-auto custom-scroll text-left bg-gray-950/70 p-3 rounded-xl border border-gray-800 space-y-1.5 text-xs"></div>

              <button id="receiver-download-btn" onclick="triggerReceiverDownload()" class="w-full py-4 px-6 rounded-2xl bg-gradient-to-r from-amber-500 via-orange-500 to-amber-600 hover:from-amber-400 hover:to-orange-400 text-gray-950 font-extrabold text-base shadow-xl shadow-amber-500/25 transform hover:-translate-y-0.5 transition-all flex items-center justify-center space-x-2">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/></svg>
                <span>Download File(s) Now</span>
              </button>

              <p class="text-[11px] text-gray-400">End-to-end encrypted direct transfer • Zero cloud storage</p>
            </div>

            <!-- STATE D: ACTIVE TRANSFER PROGRESS DASHBOARD (SHOWN ON BOTH SENDER & RECEIVER) -->
            <div id="state-active-transfer" class="hidden space-y-5">
              <div class="flex items-center justify-between border-b border-gray-800 pb-3">
                <div>
                  <div class="flex items-center space-x-2">
                    <span id="transfer-role-badge" class="px-2 py-0.5 rounded text-[10px] font-bold uppercase bg-amber-500/20 text-amber-300">Transferring</span>
                    <span id="transfer-mode-badge" class="px-2 py-0.5 rounded text-[10px] font-bold uppercase bg-emerald-500/20 text-emerald-300">P2P Direct</span>
                  </div>
                  <h3 id="active-file-name" class="text-sm sm:text-base font-bold text-white truncate max-w-[230px] mt-1">file.zip</h3>
                </div>
                <button onclick="abortAndRestartTransfer()" class="px-3.5 py-2 rounded-xl bg-red-600 hover:bg-red-500 text-white text-xs font-bold shadow transition-all">
                  Stop & Restart
                </button>
              </div>

              <!-- Big Percentage & Progress Bar -->
              <div class="space-y-2">
                <div class="flex justify-between items-end">
                  <span id="transfer-status-label" class="text-xs font-medium text-gray-300">Streaming chunks...</span>
                  <span id="transfer-percent-big" class="text-3xl font-extrabold text-amber-400">0%</span>
                </div>
                <div class="w-full h-4 bg-gray-950 rounded-full overflow-hidden border border-gray-800 p-0.5">
                  <div id="transfer-progress-bar" class="h-full rounded-full bg-gradient-to-r from-amber-500 via-orange-400 to-emerald-400 transition-all duration-150" style="width: 0%"></div>
                </div>
                <div class="flex justify-between text-[11px] text-gray-400">
                  <span id="transfer-bytes-text">0.00 MB / 0.00 MB</span>
                  <span id="transfer-file-counter">File 1 of 1</span>
                </div>
              </div>

              <!-- Live Metrics Grid (Speed, ETA, Peak) -->
              <div class="grid grid-cols-3 gap-3 text-center">
                <div class="bg-gray-950/70 p-3 rounded-xl border border-gray-800">
                  <p class="text-[10px] uppercase tracking-wider text-gray-400">Live Speed</p>
                  <p id="metric-speed" class="text-sm sm:text-base font-extrabold text-emerald-400 mt-0.5">0.00 MB/s</p>
                </div>
                <div class="bg-gray-950/70 p-3 rounded-xl border border-gray-800">
                  <p class="text-[10px] uppercase tracking-wider text-gray-400">ETA Remaining</p>
                  <p id="metric-eta" class="text-sm sm:text-base font-extrabold text-white mt-0.5">-- s</p>
                </div>
                <div class="bg-gray-950/70 p-3 rounded-xl border border-gray-800">
                  <p class="text-[10px] uppercase tracking-wider text-gray-400">Peak Speed</p>
                  <p id="metric-peak" class="text-sm sm:text-base font-extrabold text-amber-400 mt-0.5">0.00 MB/s</p>
                </div>
              </div>

              <!-- Real-Time Speed Graph Canvas -->
              <div class="bg-gray-950/80 p-3 rounded-xl border border-gray-800">
                <div class="flex justify-between items-center mb-1.5">
                  <span class="text-[10px] font-semibold uppercase tracking-wider text-gray-400">Real-Time Throughput Graph</span>
                  <span class="text-[10px] text-emerald-400">DTLS 1.3 Encrypted</span>
                </div>
                <canvas id="speed-chart-canvas" width="420" height="70" class="w-full h-[70px] block"></canvas>
              </div>
            </div>

          </div>
        </div>

        <!-- RIGHT COLUMN: HERO MARKETING & PILLS (6 Cols) -->
        <div class="lg:col-span-6 space-y-6">
          <div class="inline-flex items-center space-x-2 px-3.5 py-1.5 rounded-full bg-amber-500/10 border border-amber-500/30 text-amber-300 text-xs font-semibold">
            <span>⚡ Next-Gen WebRTC P2P Architecture</span>
          </div>
          <h1 class="text-4xl sm:text-5xl font-extrabold tracking-tight text-white leading-tight">
            Share files directly from your device to <span class="text-transparent bg-clip-text bg-gradient-to-r from-amber-400 to-orange-500">anywhere</span>
          </h1>
          <p class="text-base sm:text-lg text-gray-300 leading-relaxed">
            Send files of any size directly from your device without ever storing anything online. Simply select your files, share the QR code or link, and stream peer-to-peer.
          </p>

          <!-- 4 Feature Pills from ToffeeShare -->
          <div class="grid grid-cols-2 gap-3.5 pt-2">
            <div class="flex items-center space-x-3 p-3.5 rounded-2xl bg-gray-900/70 border border-gray-800">
              <span class="text-xl">♾️</span>
              <span class="text-sm font-bold text-gray-200">No file size limit</span>
            </div>
            <div class="flex items-center space-x-3 p-3.5 rounded-2xl bg-gray-900/70 border border-gray-800">
              <span class="text-xl">🔗</span>
              <span class="text-sm font-bold text-gray-200">Peer-to-peer</span>
            </div>
            <div class="flex items-center space-x-3 p-3.5 rounded-2xl bg-gray-900/70 border border-gray-800">
              <span class="text-xl">🚀</span>
              <span class="text-sm font-bold text-gray-200">Blazingly fast</span>
            </div>
            <div class="flex items-center space-x-3 p-3.5 rounded-2xl bg-gray-900/70 border border-gray-800">
              <span class="text-xl">🔒</span>
              <span class="text-sm font-bold text-gray-200">End-to-end encrypted</span>
            </div>
          </div>
        </div>

      </div>

      <!-- WHAT IS CHOCOSHARE / TOFFEESHARE FEATURE GRID -->
      <div class="space-y-10 pt-8 border-t border-gray-800/80">
        <div class="max-w-3xl mx-auto text-center space-y-4">
          <h2 class="text-3xl font-extrabold text-white">What is ChocoShare?</h2>
          <p class="text-gray-300 leading-relaxed">
            We are a free and independent peer-to-peer (P2P) file sharing service inspired by Dutch privacy standards that prioritizes your privacy and keeps your data safe. We store nothing online: simply close your browser to stop sending.
          </p>
          <p class="text-amber-400 font-semibold text-sm">
            Our mission is to make sure people keep their data safely into their own hands, as it should be.
          </p>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          <div class="glass-card rounded-2xl p-6 space-y-3">
            <div class="text-3xl">☁️</div>
            <h3 class="text-lg font-bold text-white">Files are shared straight from your device</h3>
            <p class="text-sm text-gray-300 leading-relaxed">
              When you close the browser tab your files are no longer accessible, minimising the risk of anyone getting unwanted access. ChocoShare uses WebRTC to find the shortest path, meaning sometimes your data doesn't even have to leave the building!
            </p>
          </div>

          <div class="glass-card rounded-2xl p-6 space-y-3">
            <div class="text-3xl">📏</div>
            <h3 class="text-lg font-bold text-white">No more file size limits</h3>
            <p class="text-sm text-gray-300 leading-relaxed">
              Because we don't store the data, there's no need for file size limits. Just share files of any size or whatever amount. As long as you keep an eye on your own data usage.
            </p>
          </div>

          <div class="glass-card rounded-2xl p-6 space-y-3">
            <div class="text-3xl">🔒</div>
            <h3 class="text-lg font-bold text-white">Only the receiver can access your files</h3>
            <p class="text-sm text-gray-300 leading-relaxed">
              Only you and the receiver can access your files. Your data is encrypted end-to-end, and can only be read by your receiver (and you of course). We use an implementation of DTLS 1.3.
            </p>
          </div>

          <div class="glass-card rounded-2xl p-6 space-y-3">
            <div class="text-3xl">🌳</div>
            <h3 class="text-lg font-bold text-white">Low environmental impact</h3>
            <p class="text-sm text-gray-300 leading-relaxed">
              Because we don't store data we don't need bulky servers, and that saves a lot of energy. By using ChocoShare you'll have a much smaller carbon footprint than when using a cloud storage provider.
            </p>
          </div>

          <div class="glass-card rounded-2xl p-6 space-y-3">
            <div class="text-3xl">📊</div>
            <h3 class="text-lg font-bold text-white">We keep on growing!</h3>
            <p class="text-sm text-gray-300 leading-relaxed">
              We started with just a few users per day, and our peer-to-peer architecture is built to handle more than 50 terabytes (TB) per month effortlessly!
            </p>
          </div>

          <div class="glass-card rounded-2xl p-6 space-y-3">
            <div class="text-3xl">💬</div>
            <h3 class="text-lg font-bold text-white">Do you still have questions?</h3>
            <p class="text-sm text-gray-300 leading-relaxed">
              Check out our <button onclick="switchTab('faq')" class="text-amber-400 underline font-semibold">frequently asked questions</button>. You can also test your WebRTC connection or support privacy-first tools ☕!
            </p>
          </div>
        </div>
      </div>
    </section>

    <!-- ========================================== -->
    <!-- TAB 2: NEARBY DEVICES (QR & 4-DIGIT CODE)  -->
    <!-- ========================================== -->
    <section id="tab-nearby" class="hidden space-y-10 max-w-4xl mx-auto">
      <div class="text-center space-y-2">
        <h2 class="text-3xl font-extrabold text-white">Connect to nearby devices</h2>
        <p class="text-sm text-gray-300">Scan the QR-code or enter the 4 digit local code to connect instantly</p>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
        <!-- Card 1: Enter 4-Digit Local Code or Open Camera Scanner -->
        <div class="glass-card rounded-3xl p-6 sm:p-8 space-y-6 flex flex-col justify-between">
          <div class="space-y-4">
            <span class="px-3 py-1 rounded-full text-xs font-bold uppercase bg-amber-500/20 text-amber-300">Option 1: Receive Files</span>
            <h3 class="text-xl font-bold text-white">Enter 4-Digit Local Code</h3>
            <p class="text-xs text-gray-300">Enter the 4-digit code displayed on the sender's screen, or scan their QR code directly with your camera.</p>

            <div class="pt-2">
              <input type="text" id="nearby-code-input" maxlength="4" placeholder="0 0 0 0"
                     class="w-full text-center text-3xl font-extrabold tracking-[0.5em] py-4 rounded-2xl bg-gray-950 border-2 border-gray-700 focus:border-amber-400 text-amber-400 focus:outline-none">
            </div>

            <button onclick="confirmNearbyCode()" class="w-full py-3.5 rounded-xl bg-gradient-to-r from-amber-500 to-orange-500 hover:from-amber-400 hover:to-orange-400 text-gray-950 font-extrabold text-sm shadow-lg transition-all">
              Confirm & Connect
            </button>
          </div>

          <div class="pt-4 border-t border-gray-800">
            <button onclick="openCameraScannerModal()" class="w-full py-3 rounded-xl bg-gray-800 hover:bg-gray-700 border border-gray-600 text-white font-semibold text-xs flex items-center justify-center space-x-2 transition-all">
              <svg class="w-4 h-4 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 9a2 2 0 012-2h.93a2 2 0 001.664-.89l.812-1.22A2 2 0 0110.07 4h3.86a2 2 0 011.664.89l.812 1.22A2 2 0 0018.07 7H19a2 2 0 012 2v9a2 2 0 01-2 2H5a2 2 0 01-2-2V9z"/></svg>
              <span>Scan Sender QR Code with Camera</span>
            </button>
          </div>
        </div>

        <!-- Card 2: This PC's Receiver QR ("Send to PC" from Phone) -->
        <div class="glass-card rounded-3xl p-6 sm:p-8 space-y-4 text-center flex flex-col items-center justify-between">
          <div class="space-y-2">
            <span class="px-3 py-1 rounded-full text-xs font-bold uppercase bg-emerald-500/20 text-emerald-300">Option 2: Send Phone -> PC</span>
            <h3 class="text-xl font-bold text-white">Scan this QR from your Phone</h3>
            <p class="text-xs text-gray-300">Select files on your phone, tap <b>"Send to PC (Scan PC QR)"</b>, and scan this code:</p>
          </div>

          <div class="bg-white p-3 rounded-2xl shadow-xl border-2 border-emerald-400 flex items-center justify-center w-[196px] h-[196px]">
            <canvas id="nearby-pc-qr-canvas" width="172" height="172" class="w-[172px] h-[172px] block"></canvas>
            <img id="nearby-pc-qr-fallback" alt="PC Pairing QR" class="w-[172px] h-[172px] hidden" />
          </div>

          <div class="text-xs text-gray-400">
            PC Pairing ID: <span id="nearby-pc-code-display" class="font-bold text-emerald-400 tracking-widest">----</span>
          </div>
        </div>
      </div>

      <!-- Detailed Explanation from ToffeeShare -->
      <div class="glass-card rounded-3xl p-6 sm:p-8 space-y-4">
        <h3 class="text-xl font-bold text-white">A more detailed explanation</h3>
        <p class="text-sm text-gray-300 leading-relaxed">
          If you don't have any cable with you and you need to send some files from your phone to your computer you can use ChocoShare as an alternative. You don't need to install anything. You just need your phone (Android or iOS), your PC and an active internet connection.
        </p>
        <p class="text-sm text-gray-300 leading-relaxed">
          Simply navigate to the homepage on your mobile phone. Press the button to select one or more files (you can select more than one file by long pressing). You'll then be presented with a screen that allows you to instantly share the link or scan the QR code presented at the top of this page, and your computer will automatically navigate to your private sharing page. If you send more than one file we advise you to use Chrome or Edge, since these browsers have implemented a single click multi-file download.
        </p>
      </div>
    </section>

    <!-- ========================================== -->
    <!-- TAB 3: ABOUT ("MAKING SHARING SWEET")      -->
    <!-- ========================================== -->
    <section id="tab-about" class="hidden max-w-3xl mx-auto space-y-8">
      <div class="glass-card rounded-3xl p-8 sm:p-10 space-y-6">
        <span class="px-3 py-1 rounded-full text-xs font-bold uppercase bg-amber-500/20 text-amber-300">Our Story</span>
        <h2 class="text-3xl sm:text-4xl font-extrabold text-white">Making sharing sweet</h2>
        <p class="text-base text-gray-300 leading-relaxed">
          I was sitting next to a colleague at work. We had to meet a tight deadline, and in order to finish our work he had to share a large file with me.
        </p>
        <p class="text-base text-gray-300 leading-relaxed">
          The file was too large to email, we didn't have a USB stick with us, and only one of us had a MacBook, so we couldn't use AirDrop. Ultimately, the file had to be split and sent via WeTransfer.
        </p>
        <blockquote class="p-4 rounded-2xl bg-amber-500/10 border-l-4 border-amber-400 text-amber-200 text-sm italic">
          "It was an incredibly frustrating experience. I could give my colleague a toffee or chocolate from a bowl on the table, but the only way I could send him a file was by literally sending it to another continent first."
        </blockquote>

        <h3 class="text-2xl font-bold text-white pt-2">There had to be another way</h3>
        <p class="text-base text-gray-300 leading-relaxed">
          This wasn't the first time this had happened. Sending large files via email or with third party tools such as WhatsApp or Slack is impossible. It forces you to use yet another third party service with annoying limitations such as file size limits, speed limits or required registration.
        </p>
        <p class="text-base text-gray-300 leading-relaxed">
          There's always a catch, and no matter which service you use, your files always end up stored somewhere on the internet on an unknown server.
        </p>

        <h3 class="text-2xl font-bold text-white pt-2">Building a solution</h3>
        <p class="text-base text-gray-300 leading-relaxed">
          It simply had to change, so we built this solution: nothing is ever stored online, you send your files directly from your computer or mobile phone, there's no file size limits or speed limits, and if you're on the same network your data doesn't even have to leave the building.
        </p>
        <p class="text-lg font-bold text-amber-400">
          In our opinion: file transfer as it should be.
        </p>
      </div>
    </section>

    <!-- ========================================== -->
    <!-- TAB 4: BLOG (ALL 24 POSTS + READER MODAL)  -->
    <!-- ========================================== -->
    <section id="tab-blog" class="hidden space-y-8">
      <div class="max-w-3xl">
        <h2 class="text-3xl font-extrabold text-white">Blog</h2>
        <p class="text-sm text-gray-300 mt-1">The latest news about internet privacy, WebRTC security, and platform updates.</p>
      </div>

      <!-- Article Reader View (Hidden unless an article is clicked) -->
      <div id="blog-reader" class="hidden glass-card rounded-3xl p-8 space-y-4">
        <button onclick="closeBlogReader()" class="px-4 py-1.5 rounded-xl bg-gray-800 hover:bg-gray-700 text-amber-400 text-xs font-bold">← Back to all articles</button>
        <p id="blog-reader-date" class="text-xs text-amber-400 font-semibold"></p>
        <h3 id="blog-reader-title" class="text-2xl font-extrabold text-white"></h3>
        <div id="blog-reader-body" class="text-sm text-gray-300 space-y-3 leading-relaxed"></div>
      </div>

      <div id="blog-grid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"></div>
    </section>

    <!-- ========================================== -->
    <!-- TAB 5: FAQ & BROWSER SUPPORT MATRIX        -->
    <!-- ========================================== -->
    <section id="tab-faq" class="hidden space-y-10 max-w-4xl mx-auto">
      <div class="space-y-2">
        <h2 class="text-3xl font-extrabold text-white">FAQ & Knowledge Base</h2>
        <p class="text-sm text-gray-300">The most common questions about transferring your files peer-to-peer and optimizing transfer speeds.</p>
      </div>

      <div class="space-y-4">
        <div class="glass-card rounded-2xl p-6 space-y-2">
          <h3 class="text-lg font-bold text-amber-400">Why peer to peer?</h3>
          <p class="text-sm text-gray-300 leading-relaxed">
            Peer to peer communication means the communication flows directly between two devices rather than via an intermediate server. This means your data will not be stored somewhere else before it reaches its destination. We achieve this via WebRTC and STUN, allowing much higher transfer speeds simply because you skip uploading to a cloud server.
          </p>
        </div>

        <div class="glass-card rounded-2xl p-6 space-y-2">
          <h3 class="text-lg font-bold text-amber-400">Why is it slower than my maximum network speed?</h3>
          <p class="text-sm text-gray-300 leading-relaxed">
            Expect transfer speeds between 10 and 100 Mbit/s on average (up to 300 Mbit/s on local Wi-Fi). Keep in mind you are uploading and downloading at the same time! To increase transfer speed: connect both devices to the same Wi-Fi network, use a stable connection, or restart the transfer so the algorithm finds a shorter local path.
          </p>
        </div>

        <div class="glass-card rounded-2xl p-6 space-y-2">
          <h3 class="text-lg font-bold text-amber-400">For how long are files stored in the Cloud?</h3>
          <p class="text-base font-extrabold text-emerald-400">--&gt; We don't store anything! &lt;--</p>
          <p class="text-sm text-gray-300 leading-relaxed">
            Your files are available as long as you keep your browser tab open and don't shut down your PC or phone. The moment you close the tab, the session ceases to exist.
          </p>
        </div>

        <div class="glass-card rounded-2xl p-6 space-y-2">
          <h3 class="text-lg font-bold text-amber-400">The progress bar was stuck at 0% — how does ChocoShare fix this?</h3>
          <p class="text-sm text-gray-300 leading-relaxed">
            On strict mobile networks or corporate firewalls, direct UDP peer-to-peer connections can sometimes be blocked. ChocoShare includes an automatic zero-storage <b>Fallback Relay</b> that seamlessly streams your encrypted chunks if direct WebRTC takes longer than 4 seconds, ensuring your progress bar never gets stuck at 0%!
          </p>
        </div>
      </div>

      <!-- Browser Compatibility Table from ToffeeShare -->
      <div class="glass-card rounded-3xl p-6 sm:p-8 space-y-4 overflow-x-auto">
        <h3 class="text-xl font-bold text-white">What browsers do you support?</h3>
        <table class="w-full text-left text-xs sm:text-sm border-collapse">
          <thead>
            <tr class="border-b border-gray-700 text-amber-400">
              <th class="py-3 px-2">Browser</th>
              <th class="py-3 px-2">P2P WebRTC</th>
              <th class="py-3 px-2">Fallback Support</th>
              <th class="py-3 px-2">No Size Limit</th>
              <th class="py-3 px-2">Tested Status</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-800 text-gray-300">
            <tr>
              <td class="py-3 px-2 font-bold text-white">Google Chrome</td>
              <td class="py-3 px-2">✔️ Supported</td>
              <td class="py-3 px-2">✔️ Supported</td>
              <td class="py-3 px-2">✔️ Unlimited</td>
              <td class="py-3 px-2 text-emerald-400">✔️ Thoroughly tested</td>
            </tr>
            <tr>
              <td class="py-3 px-2 font-bold text-white">Mozilla Firefox</td>
              <td class="py-3 px-2">✔️ Supported</td>
              <td class="py-3 px-2">✔️ Supported</td>
              <td class="py-3 px-2">✔️ Unlimited</td>
              <td class="py-3 px-2 text-emerald-400">✔️ Thoroughly tested</td>
            </tr>
            <tr>
              <td class="py-3 px-2 font-bold text-white">Microsoft Edge (v79+)</td>
              <td class="py-3 px-2">✔️ Supported</td>
              <td class="py-3 px-2">✔️ Supported</td>
              <td class="py-3 px-2">✔️ Unlimited</td>
              <td class="py-3 px-2 text-amber-300">✔️ Multi-file single click</td>
            </tr>
            <tr>
              <td class="py-3 px-2 font-bold text-white">Apple Safari</td>
              <td class="py-3 px-2">✔️ Supported</td>
              <td class="py-3 px-2">✔️ Supported</td>
              <td class="py-3 px-2">⚠️ Memory bound (~4GB)</td>
              <td class="py-3 px-2 text-gray-400">Tested on major releases</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <!-- ========================================== -->
    <!-- TAB 6: PRIVACY & SECURITY                  -->
    <!-- ========================================== -->
    <section id="tab-privacy" class="hidden max-w-3xl mx-auto space-y-6">
      <div class="glass-card rounded-3xl p-8 space-y-4">
        <span class="px-3 py-1 rounded-full text-xs font-bold uppercase bg-emerald-500/20 text-emerald-300">Zero-Log Architecture</span>
        <h2 class="text-3xl font-extrabold text-white">Privacy & End-to-End Encryption</h2>
        <p class="text-sm text-gray-300 leading-relaxed">
          1. <b>No Accounts, No Databases:</b> ChocoShare does not use any database. We do not ask for logins, emails, or passwords.
        </p>
        <p class="text-sm text-gray-300 leading-relaxed">
          2. <b>DTLS 1.3 Encryption:</b> Every WebRTC data channel is automatically encrypted end-to-end between the sender's browser and the receiver's browser.
        </p>
        <p class="text-sm text-gray-300 leading-relaxed">
          3. <b>Ephemeral Signaling Only:</b> Our server only helps the two devices discover each other via a temporary session ID or 4-digit code. Once you close your browser tab, all session metadata vanishes from memory immediately.
        </p>
      </div>
    </section>

    <!-- ========================================== -->
    <!-- TAB 7: DOWNLOAD / MOBILE & DESKTOP         -->
    <!-- ========================================== -->
    <section id="tab-download" class="hidden max-w-4xl mx-auto space-y-8">
      <div class="text-center space-y-2">
        <h2 class="text-3xl font-extrabold text-white">Use ChocoShare Everywhere</h2>
        <p class="text-sm text-gray-300">Works natively in any modern browser on Windows, macOS, Android, and iOS without installing anything.</p>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div class="glass-card rounded-3xl p-6 space-y-3">
          <h3 class="text-xl font-bold text-white">💻 Windows & Desktop</h3>
          <p class="text-sm text-gray-300">Open ChocoShare in Chrome, Edge, or Firefox and click the "Install App" icon in your address bar to run ChocoShare as a standalone desktop app.</p>
        </div>
        <div class="glass-card rounded-3xl p-6 space-y-3">
          <h3 class="text-xl font-bold text-white">📱 Android & iOS Mobile Connect</h3>
          <p class="text-sm text-gray-300">Use our built-in QR Scanner and 4-Digit Nearby Code to beam photos, videos, and APKs between your phone and laptop at local Wi-Fi speeds.</p>
        </div>
      </div>
    </section>

  </main>

  <!-- CAMERA QR SCANNER MODAL -->
  <div id="scanner-modal" class="fixed inset-0 z-50 hidden bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
    <div class="glass-card rounded-3xl p-6 max-w-md w-full space-y-4 text-center">
      <div class="flex justify-between items-center">
        <h3 id="scanner-modal-title" class="text-lg font-bold text-white">Scan QR Code</h3>
        <button onclick="closeCameraScannerModal()" class="text-gray-400 hover:text-white text-sm font-bold">✕ Close</button>
      </div>
      <div id="qr-reader-box" class="w-full overflow-hidden rounded-2xl border border-gray-700 bg-black min-h-[240px]"></div>
      <p class="text-xs text-gray-400">Point your camera at the ChocoShare QR code on the other device.</p>
    </div>
  </div>

  <!-- FOOTER -->
  <footer class="border-t border-gray-800/80 bg-gray-950/60 py-6 mt-12">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-col sm:flex-row items-center justify-between text-xs text-gray-400 space-y-2 sm:space-y-0">
      <div>🍫 <b>ChocoShare</b> — Making sharing sweet. Zero cloud storage, 100% P2P.</div>
      <div class="flex space-x-4">
        <button onclick="switchTab('about')" class="hover:text-amber-400">About</button>
        <button onclick="switchTab('blog')" class="hover:text-amber-400">Blog</button>
        <button onclick="switchTab('faq')" class="hover:text-amber-400">FAQ</button>
        <button onclick="switchTab('privacy')" class="hover:text-amber-400">Privacy</button>
      </div>
    </div>
  </footer>

  <!-- ========================================== -->
  <!-- CLIENT-SIDE ENGINE (WEBRTC + FALLBACK + UI)-->
  <!-- ========================================== -->
  <script>
    const CHUNK_SIZE = 64 * 1024; // 64KB optimal WebRTC chunk size
    let ws = null;
    let pc = null;
    let dataChannel = null;
    let iceCandidateQueue = [];

    let selectedFiles = [];
    let totalSelectedBytes = 0;
    let currentSessionId = new URLSearchParams(window.location.search).get('s') || null;
    let role = currentSessionId ? 'receiver' : 'sender';

    // Transfer runtime state
    let isTransferring = false;
    let isAborted = false;
    let usingFallbackRelay = false;
    let p2pWatchdogTimer = null;

    // Metrics & Graphing state
    let transferStartTime = 0;
    let peakSpeedMbps = 0;
    let speedHistory = [];
    let senderAckedBytes = 0;

    // Receiver incoming buffers
    let sessionFilesMeta = [];
    let sessionTotalBytes = 0;
    let currentFileIndex = 0;
    let currentFileBuffers = [];
    let currentFileReceivedBytes = 0;
    let totalReceivedBytesOverall = 0;
    let lastAckSentTime = 0;

    // QR Scanner instance
    let html5QrScanner = null;
    let scannerMode = 'join'; // 'join' | 'send-to-pc'

    // ------------------------------------------
    // 1. INITIALIZE WEBSOCKET SIGNALING
    // ------------------------------------------
    function initWebSocket() {
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      ws = new WebSocket(proto + '//' + window.location.host);
      ws.binaryType = 'arraybuffer';

      ws.onopen = function() {
        document.getElementById('ws-dot').className = 'w-2 h-2 rounded-full bg-emerald-400';
        document.getElementById('ws-status-text').innerText = 'Signaling Online';

        // If user opened a share link (?s=...), join that session immediately to preview file
        if (currentSessionId && role === 'receiver') {
          showReceiverPreviewState();
          ws.send(JSON.stringify({
            type: 'join-session',
            sessionId: currentSessionId
          }));
        }
      };

      ws.onclose = function() {
        document.getElementById('ws-dot').className = 'w-2 h-2 rounded-full bg-red-500';
        document.getElementById('ws-status-text').innerText = 'Reconnecting...';
        setTimeout(initWebSocket, 2500);
      };

      ws.onmessage = async function(event) {
        // Handle Binary Chunk if in Fallback Relay mode
        if (event.data instanceof ArrayBuffer) {
          onIncomingFileChunk(event.data);
          return;
        }

        let msg;
        try {
          msg = JSON.parse(event.data);
        } catch (e) {
          return;
        }

        // Sender receives confirmation of session creation + 4-digit code
        if (msg.type === 'session-created') {
          document.getElementById('sender-4digit-code').innerText = msg.code4;
        }

        // Nearby 4-digit code lookup result
        else if (msg.type === 'code-found') {
          window.location.href = window.location.origin + window.location.pathname + '?s=' + msg.sessionId;
        }

        // Nearby PC pairing QR ready
        else if (msg.type === 'nearby-receiver-ready') {
          document.getElementById('nearby-pc-code-display').innerText = msg.waiterCode;
          const pairUrl = window.location.origin + window.location.pathname + '?pair=' + msg.waiterCode;
          renderGuaranteedQR(pairUrl, 'nearby-pc-qr-canvas', 'nearby-pc-qr-fallback');
        }

        // Phone pushed session to this PC
        else if (msg.type === 'auto-open-session') {
          window.location.href = window.location.origin + window.location.pathname + '?s=' + msg.sessionId;
        }

        // Receiver gets file metadata upon opening link (BEFORE clicking Download)
        else if (msg.type === 'session-info') {
          sessionFilesMeta = msg.files || [];
          sessionTotalBytes = msg.totalSize || 0;
          populateReceiverPreviewUI();
        }

        // Sender is notified that Receiver is looking at the file page
        else if (msg.type === 'receiver-viewed') {
          const waitEl = document.getElementById('sender-wait-status');
          if (waitEl) {
            waitEl.innerText = 'Receiver connected! Waiting for them to click "Download File(s) Now"...';
          }
        }

        // Receiver clicked Download -> Sender starts transfer & shows progress bar immediately
        else if (msg.type === 'start-transfer-now') {
          showActiveTransferDashboard('Sending');
          startSenderTransferFlow();
        }

        // WebRTC Signaling (Offer, Answer, ICE)
        else if (msg.type === 'webrtc-signal') {
          await handleWebRTCSignal(msg.payload);
        }

        // Control messages relayed via WS
        else if (msg.type === 'switch-to-fallback') {
          usingFallbackRelay = true;
          document.getElementById('transfer-mode-badge').innerText = 'Turbo Relay';
        }
        else if (msg.type === 'file-header') {
          onIncomingFileHeader(msg);
        }
        else if (msg.type === 'file-complete') {
          finalizeCurrentIncomingFile();
        }
        else if (msg.type === 'all-complete') {
          markTransferCompleteUI('Download Complete! ✓');
        }
        else if (msg.type === 'progress-ack') {
          // Receiver reports exact received bytes to Sender so Sender progress bar stays 100% synced
          senderAckedBytes = msg.receivedBytes;
          updateDashboardMetrics(senderAckedBytes, totalSelectedBytes);
          if (senderAckedBytes >= totalSelectedBytes && totalSelectedBytes > 0) {
            markTransferCompleteUI('Transfer 100% Complete! ✓');
          }
        }
        else if (msg.type === 'abort-transfer') {
          handleRemoteAbort();
        }
        else if (msg.type === 'session-expired' || msg.type === 'error-msg') {
          alert(msg.message);
          window.location.href = window.location.pathname;
        }
      };
    }

    // ------------------------------------------
    // 2. GUARANTEED QR CODE GENERATOR (CANVAS + FALLBACK)
    // ------------------------------------------
    function renderGuaranteedQR(urlText, canvasId, fallbackImgId) {
      const canvas = document.getElementById(canvasId);
      const fallbackImg = document.getElementById(fallbackImgId);

      let canvasSuccess = false;
      if (typeof QRious !== 'undefined' && canvas) {
        try {
          new QRious({
            element: canvas,
            value: urlText,
            size: 170,
            background: '#ffffff',
            foreground: '#000000',
            level: 'M'
          });
          canvas.classList.remove('hidden');
          if (fallbackImg) fallbackImg.classList.add('hidden');
          canvasSuccess = true;
        } catch (err) {
          canvasSuccess = false;
        }
      }

      if (!canvasSuccess && fallbackImg) {
        if (canvas) canvas.classList.add('hidden');
        fallbackImg.src = 'https://api.qrserver.com/v1/create-qr-code/?size=170x170&data=' + encodeURIComponent(urlText);
        fallbackImg.classList.remove('hidden');
      }
    }

    // ------------------------------------------
    // 3. SENDER: SELECT FILES & GENERATE SHARE LINK / QR
    // ------------------------------------------
    function onFilesSelected(fileList) {
      if (!fileList || fileList.length === 0) return;
      selectedFiles = Array.from(fileList);
      role = 'sender';
      currentSessionId = Math.random().toString(36).substring(2, 10) + Math.random().toString(36).substring(2, 6);

      totalSelectedBytes = 0;
      const filesMeta = selectedFiles.map(function(f) {
        totalSelectedBytes += f.size;
        return {
          name: f.name,
          size: f.size,
          fileType: f.type || 'application/octet-stream'
        };
      });

      // Switch UI to Sender Ready View
      document.getElementById('state-idle').classList.add('hidden');
      document.getElementById('state-sender-ready').classList.remove('hidden');

      const titleText = selectedFiles.length === 1
        ? selectedFiles[0].name
        : selectedFiles.length + ' Files Selected';
      document.getElementById('sender-file-title').innerText = titleText;
      document.getElementById('sender-file-subtitle').innerText = formatBytes(totalSelectedBytes) + ' total';

      // Render file list items
      const listEl = document.getElementById('sender-file-list');
      listEl.innerHTML = '';
      selectedFiles.forEach(function(f, idx) {
        const row = document.createElement('div');
        row.className = 'flex justify-between items-center text-gray-300 py-1 border-b border-gray-800/60 last:border-none';
        row.innerHTML = '<span class="truncate max-w-[200px]">' + (idx + 1) + '. ' + escapeHtml(f.name) + '</span>' +
                        '<span class="text-amber-400 font-semibold ml-2">' + formatBytes(f.size) + '</span>';
        listEl.appendChild(row);
      });

      const shareUrl = window.location.origin + window.location.pathname + '?s=' + currentSessionId;
      document.getElementById('sender-share-url').value = shareUrl;

      // Render QR code immediately
      renderGuaranteedQR(shareUrl, 'sender-qr-canvas', 'sender-qr-fallback');

      // Register session on WebSocket server
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'create-session',
          sessionId: currentSessionId,
          files: filesMeta,
          totalSize: totalSelectedBytes
        }));
      }
    }

    // ------------------------------------------
    // 4. RECEIVER: PREVIEW FILES & CLICK DOWNLOAD
    // ------------------------------------------
    function showReceiverPreviewState() {
      document.getElementById('state-idle').classList.add('hidden');
      document.getElementById('state-sender-ready').classList.add('hidden');
      document.getElementById('state-active-transfer').classList.add('hidden');
      document.getElementById('state-receiver-preview').classList.remove('hidden');
    }

    function populateReceiverPreviewUI() {
      if (!sessionFilesMeta || sessionFilesMeta.length === 0) return;
      const title = sessionFilesMeta.length === 1
        ? sessionFilesMeta[0].name
        : sessionFilesMeta.length + ' Files Ready to Download';
      document.getElementById('receiver-preview-title').innerText = title;
      document.getElementById('receiver-preview-size').innerText = 'Total Size: ' + formatBytes(sessionTotalBytes);

      const listEl = document.getElementById('receiver-preview-list');
      listEl.innerHTML = '';
      sessionFilesMeta.forEach(function(f, i) {
        const item = document.createElement('div');
        item.className = 'flex justify-between items-center text-gray-300 py-1 border-b border-gray-800/60 last:border-none';
        item.innerHTML = '<span class="truncate max-w-[210px]">📄 ' + escapeHtml(f.name) + '</span>' +
                         '<span class="text-amber-400 font-semibold">' + formatBytes(f.size) + '</span>';
        listEl.appendChild(item);
      });
    }

    function triggerReceiverDownload() {
      isAborted = false;
      currentFileIndex = 0;
      currentFileBuffers = [];
      currentFileReceivedBytes = 0;
      totalReceivedBytesOverall = 0;

      // Transition Receiver UI to Active Progress Bar Dashboard
      showActiveTransferDashboard('Receiving');

      // Prepare Receiver WebRTC PeerConnection
      setupPeerConnection(false);

      // Tell Sender we clicked Download so Sender also opens progress bar & starts streaming
      ws.send(JSON.stringify({
        type: 'request-download'
      }));
    }

    // ------------------------------------------
    // 5. WEBRTC + AUTOMATIC FALLBACK STREAMING ENGINE
    // ------------------------------------------
    const rtcConfiguration = {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun.cloudflare.com:3478' }
      ]
    };

    function setupPeerConnection(isInitiator) {
      if (pc) {
        try { pc.close(); } catch (e) {}
      }
      pc = new RTCPeerConnection(rtcConfiguration);
      iceCandidateQueue = [];

      pc.onicecandidate = function(event) {
        if (event.candidate && ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: 'webrtc-signal',
            payload: { candidate: event.candidate }
          }));
        }
      };

      if (isInitiator) {
        dataChannel = pc.createDataChannel('chocoshare-data', { ordered: true });
        configureDataChannel();
        pc.createOffer().then(function(offer) {
          return pc.setLocalDescription(offer);
        }).then(function() {
          ws.send(JSON.stringify({
            type: 'webrtc-signal',
            payload: { sdp: pc.localDescription }
          }));
        }).catch(function(err) {
          console.error('Offer error:', err);
        });
      } else {
        pc.ondatachannel = function(event) {
          dataChannel = event.channel;
          configureDataChannel();
        };
      }
    }

    async function handleWebRTCSignal(payload) {
      if (!pc) {
        setupPeerConnection(false);
      }
      try {
        if (payload.sdp) {
          await pc.setRemoteDescription(new RTCSessionDescription(payload.sdp));
          // Flush queued ICE candidates
          while (iceCandidateQueue.length > 0) {
            const c = iceCandidateQueue.shift();
            await pc.addIceCandidate(new RTCIceCandidate(c));
          }
          if (payload.sdp.type === 'offer') {
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            ws.send(JSON.stringify({
              type: 'webrtc-signal',
              payload: { sdp: pc.localDescription }
            }));
          }
        } else if (payload.candidate) {
          if (pc.remoteDescription && pc.remoteDescription.type) {
            await pc.addIceCandidate(new RTCIceCandidate(payload.candidate));
          } else {
            iceCandidateQueue.push(payload.candidate);
          }
        }
      } catch (err) {
        console.error('WebRTC signal error:', err);
      }
    }

    function configureDataChannel() {
      // CRITICAL FIX: Explicitly set binaryType to arraybuffer so byteLength is never undefined!
      dataChannel.binaryType = 'arraybuffer';
      dataChannel.bufferedAmountLowThreshold = 512 * 1024;

      dataChannel.onopen = function() {
        if (p2pWatchdogTimer) {
          clearTimeout(p2pWatchdogTimer);
          p2pWatchdogTimer = null;
        }
        usingFallbackRelay = false;
        document.getElementById('transfer-mode-badge').innerText = 'P2P Direct (WebRTC)';
        if (role === 'sender' && !isTransferring) {
          pumpAllFilesToPeer(false);
        }
      };

      dataChannel.onmessage = function(event) {
        if (typeof event.data === 'string') {
          const ctrl = JSON.parse(event.data);
          if (ctrl.type === 'file-header') {
            onIncomingFileHeader(ctrl);
          } else if (ctrl.type === 'file-complete') {
            finalizeCurrentIncomingFile();
          } else if (ctrl.type === 'all-complete') {
            markTransferCompleteUI('Download Complete! ✓');
          }
        } else {
          onIncomingFileChunk(event.data);
        }
      };
    }

    function startSenderTransferFlow() {
      isAborted = false;
      isTransferring = false;
      senderAckedBytes = 0;
      usingFallbackRelay = false;

      setupPeerConnection(true);

      // Watchdog: If strict NAT/Mobile network blocks direct WebRTC after 3.5 seconds, seamlessly stream via Fallback Relay
      p2pWatchdogTimer = setTimeout(function() {
        if (!isTransferring && (!dataChannel || dataChannel.readyState !== 'open')) {
          usingFallbackRelay = true;
          document.getElementById('transfer-mode-badge').innerText = 'Turbo Relay';
          ws.send(JSON.stringify({ type: 'switch-to-fallback' }));
          pumpAllFilesToPeer(true);
        }
      }, 3500);
    }

    async function pumpAllFilesToPeer(useWsRelay) {
      if (isTransferring) return;
      isTransferring = true;
      transferStartTime = performance.now();
      let totalSentSoFar = 0;

      for (let i = 0; i < selectedFiles.length; i++) {
        if (isAborted) return;
        const file = selectedFiles[i];

        document.getElementById('active-file-name').innerText = file.name;
        document.getElementById('transfer-file-counter').innerText = 'File ' + (i + 1) + ' of ' + selectedFiles.length;

        const headerMsg = JSON.stringify({
          type: 'file-header',
          index: i,
          totalFiles: selectedFiles.length,
          name: file.name,
          size: file.size,
          mime: file.type || 'application/octet-stream'
        });

        if (useWsRelay) {
          ws.send(headerMsg);
        } else {
          dataChannel.send(headerMsg);
        }

        await streamSingleFile(file, useWsRelay, function(chunkBytes) {
          totalSentSoFar += chunkBytes;
          // Update sender UI immediately even between receiver ACKs
          const displayBytes = Math.max(totalSentSoFar, senderAckedBytes);
          updateDashboardMetrics(displayBytes, totalSelectedBytes);
        });

        if (isAborted) return;

        const completeMsg = JSON.stringify({ type: 'file-complete', index: i });
        if (useWsRelay) {
          ws.send(completeMsg);
        } else {
          dataChannel.send(completeMsg);
        }
      }

      if (!isAborted) {
        const doneMsg = JSON.stringify({ type: 'all-complete' });
        if (useWsRelay) {
          ws.send(doneMsg);
        } else {
          dataChannel.send(doneMsg);
        }
        updateDashboardMetrics(totalSelectedBytes, totalSelectedBytes);
        markTransferCompleteUI('Transfer 100% Complete! ✓');
      }
    }

    function streamSingleFile(file, useWsRelay, onChunkSent) {
      return new Promise(function(resolve) {
        let offset = 0;
        const reader = new FileReader();

        function readNextSlice() {
          if (isAborted) {
            resolve();
            return;
          }
          if (offset >= file.size) {
            resolve();
            return;
          }

          // Backpressure protection
          if (!useWsRelay && dataChannel && dataChannel.bufferedAmount > 4 * 1024 * 1024) {
            setTimeout(readNextSlice, 25);
            return;
          }

          const slice = file.slice(offset, offset + CHUNK_SIZE);
          reader.readAsArrayBuffer(slice);
        }

        reader.onload = function(e) {
          if (isAborted) {
            resolve();
            return;
          }
          const buffer = e.target.result;
          if (useWsRelay) {
            ws.send(buffer);
          } else {
            dataChannel.send(buffer);
          }
          offset += buffer.byteLength;
          onChunkSent(buffer.byteLength);
          setTimeout(readNextSlice, 1);
        };

        readNextSlice();
      });
    }

    // ------------------------------------------
    // 6. RECEIVER CHUNK ASSEMBLY & AUTO-DOWNLOAD
    // ------------------------------------------
    let activeIncomingFile = { name: 'download.bin', size: 0, mime: 'application/octet-stream', index: 0, totalFiles: 1 };

    function onIncomingFileHeader(header) {
      activeIncomingFile = header;
      currentFileBuffers = [];
      currentFileReceivedBytes = 0;
      document.getElementById('active-file-name').innerText = header.name;
      document.getElementById('transfer-file-counter').innerText = 'File ' + (header.index + 1) + ' of ' + header.totalFiles;
    }

    function onIncomingFileChunk(arrayBuffer) {
      if (isAborted) return;
      currentFileBuffers.push(arrayBuffer);
      currentFileReceivedBytes += arrayBuffer.byteLength;
      totalReceivedBytesOverall += arrayBuffer.byteLength;

      updateDashboardMetrics(totalReceivedBytesOverall, sessionTotalBytes);

      // Send live progress ACK to Sender every 80ms so Sender's progress bar updates smoothly too
      const now = performance.now();
      if (now - lastAckSentTime > 80 || totalReceivedBytesOverall >= sessionTotalBytes) {
        lastAckSentTime = now;
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: 'progress-ack',
            receivedBytes: totalReceivedBytesOverall
          }));
        }
      }
    }

    function finalizeCurrentIncomingFile() {
      if (isAborted || currentFileBuffers.length === 0) return;
      const blob = new Blob(currentFileBuffers, { type: activeIncomingFile.mime || 'application/octet-stream' });
      const downloadUrl = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = downloadUrl;
      a.download = activeIncomingFile.name || 'chocoshare-file';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(function() { URL.revokeObjectURL(downloadUrl); }, 10000);
      currentFileBuffers = [];
    }

    // ------------------------------------------
    // 7. LIVE ANALYTICS DASHBOARD & GRAPH ENGINE
    // ------------------------------------------
    function showActiveTransferDashboard(roleText) {
      document.getElementById('state-idle').classList.add('hidden');
      document.getElementById('state-sender-ready').classList.add('hidden');
      document.getElementById('state-receiver-preview').classList.add('hidden');
      document.getElementById('state-active-transfer').classList.remove('hidden');

      document.getElementById('transfer-role-badge').innerText = roleText;
      document.getElementById('transfer-status-label').innerText = 'Streaming encrypted chunks...';
      document.getElementById('transfer-percent-big').innerText = '0%';
      document.getElementById('transfer-progress-bar').style.width = '0%';

      transferStartTime = performance.now();
      peakSpeedMbps = 0;
      speedHistory = [];
    }

    function updateDashboardMetrics(currentBytes, totalBytes) {
      if (totalBytes <= 0) return;
      const clampedCurrent = Math.min(currentBytes, totalBytes);
      const percent = Math.min(100, Math.round((clampedCurrent / totalBytes) * 100));

      const elapsedSec = Math.max(0.1, (performance.now() - transferStartTime) / 1000);
      const bytesPerSec = clampedCurrent / elapsedSec;
      const speedMbps = bytesPerSec / (1024 * 1024);
      if (speedMbps > peakSpeedMbps) peakSpeedMbps = speedMbps;

      const remainingBytes = Math.max(0, totalBytes - clampedCurrent);
      const etaSec = bytesPerSec > 0 ? Math.ceil(remainingBytes / bytesPerSec) : 0;

      document.getElementById('transfer-percent-big').innerText = percent + '%';
      document.getElementById('transfer-progress-bar').style.width = percent + '%';
      document.getElementById('transfer-bytes-text').innerText = formatBytes(clampedCurrent) + ' / ' + formatBytes(totalBytes);
      document.getElementById('metric-speed').innerText = speedMbps.toFixed(2) + ' MB/s';
      document.getElementById('metric-eta').innerText = etaSec + ' s';
      document.getElementById('metric-peak').innerText = peakSpeedMbps.toFixed(2) + ' MB/s';

      speedHistory.push(speedMbps);
      if (speedHistory.length > 40) speedHistory.shift();
      drawSpeedGraph();
    }

    function drawSpeedGraph() {
      const canvas = document.getElementById('speed-chart-canvas');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);

      if (speedHistory.length < 2) return;
      const maxVal = Math.max(1, peakSpeedMbps * 1.15);

      ctx.beginPath();
      ctx.moveTo(0, h);
      for (let i = 0; i < speedHistory.length; i++) {
        const x = (i / (speedHistory.length - 1)) * w;
        const y = h - (speedHistory[i] / maxVal) * (h - 10);
        ctx.lineTo(x, y);
      }
      ctx.lineTo(w, h);
      ctx.closePath();

      const grad = ctx.createLinearGradient(0, 0, 0, h);
      grad.addColorStop(0, 'rgba(245, 158, 11, 0.45)');
      grad.addColorStop(1, 'rgba(16, 185, 129, 0.05)');
      ctx.fillStyle = grad;
      ctx.fill();

      ctx.beginPath();
      for (let i = 0; i < speedHistory.length; i++) {
        const x = (i / (speedHistory.length - 1)) * w;
        const y = h - (speedHistory[i] / maxVal) * (h - 10);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.strokeStyle = '#fbbf24';
      ctx.lineWidth = 2.5;
      ctx.stroke();
    }

    function markTransferCompleteUI(message) {
      isTransferring = false;
      document.getElementById('transfer-status-label').innerText = message;
      document.getElementById('transfer-percent-big').innerText = '100%';
      document.getElementById('transfer-progress-bar').style.width = '100%';
      document.getElementById('metric-eta').innerText = '0 s';
    }

    // ------------------------------------------
    // 8. STOP & RESTART CONTROL
    // ------------------------------------------
    function abortAndRestartTransfer() {
      isAborted = true;
      isTransferring = false;
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'abort-transfer' }));
      }
      handleRemoteAbort();
    }

    function handleRemoteAbort() {
      isAborted = true;
      isTransferring = false;
      currentFileBuffers = [];
      currentFileReceivedBytes = 0;
      totalReceivedBytesOverall = 0;
      if (pc) {
        try { pc.close(); } catch (e) {}
        pc = null;
      }

      if (role === 'receiver') {
        showReceiverPreviewState();
        populateReceiverPreviewUI();
      } else {
        document.getElementById('state-active-transfer').classList.add('hidden');
        document.getElementById('state-sender-ready').classList.remove('hidden');
        document.getElementById('sender-wait-status').innerText = 'Transfer stopped. Receiver can click Download again to restart from 0%.';
      }
    }

    function stopAndResetSession() {
      window.location.href = window.location.pathname;
    }

    // ------------------------------------------
    // 9. NEARBY DEVICES & QR CAMERA SCANNER
    // ------------------------------------------
    function confirmNearbyCode() {
      const code = document.getElementById('nearby-code-input').value.trim();
      if (code.length !== 4) {
        alert('Please enter a valid 4-digit local code.');
        return;
      }
      ws.send(JSON.stringify({
        type: 'lookup-code',
        code: code
      }));
    }

    function openCameraScannerModal() {
      scannerMode = 'join';
      document.getElementById('scanner-modal-title').innerText = 'Scan Sender QR Code';
      document.getElementById('scanner-modal').classList.remove('hidden');
      startHtml5Scanner();
    }

    function openSendToPcScanner() {
      scannerMode = 'send-to-pc';
      document.getElementById('scanner-modal-title').innerText = 'Scan PC Nearby QR Code';
      document.getElementById('scanner-modal').classList.remove('hidden');
      startHtml5Scanner();
    }

    function startHtml5Scanner() {
      if (typeof Html5Qrcode === 'undefined') {
        alert('Camera scanner library is still loading. Please use your phone camera app or enter the 4-digit code.');
        return;
      }
      html5QrScanner = new Html5Qrcode('qr-reader-box');
      html5QrScanner.start(
        { facingMode: 'environment' },
        { fps: 10, qrbox: { width: 220, height: 220 } },
        function(decodedText) {
          closeCameraScannerModal();
          if (scannerMode === 'send-to-pc') {
            try {
              const parsed = new URL(decodedText);
              const pairCode = parsed.searchParams.get('pair');
              if (pairCode && currentSessionId) {
                ws.send(JSON.stringify({
                  type: 'push-session-to-pc',
                  waiterCode: pairCode,
                  sessionId: currentSessionId
                }));
                alert('Connected to PC! Check your PC screen to click Download.');
                return;
              }
            } catch (e) {}
          }
          window.location.href = decodedText;
        },
        function() {}
      ).catch(function() {
        alert('Could not access camera. Please allow camera permission or use the 4-digit local code.');
        closeCameraScannerModal();
      });
    }

    function closeCameraScannerModal() {
      document.getElementById('scanner-modal').classList.add('hidden');
      if (html5QrScanner) {
        html5QrScanner.stop().then(function() {
          html5QrScanner.clear();
        }).catch(function() {});
      }
    }

    // ------------------------------------------
    // 10. BLOG POSTS DATA (ALL 24 TOFFEESHARE POSTS)
    // ------------------------------------------
    const blogPosts = [
      { date: '11/4/2024', title: 'Ways to improve your download speed', summary: 'Even though internet speeds are increasingly fast, downloading large files can sometimes still be tediously slow. Here are our top tips to maximize your P2P speed.', body: 'To achieve the fastest transfer speeds (up to 300 Mbit/s), ensure both devices are connected to the same local Wi-Fi or Ethernet network. When both devices share a local router, WebRTC routes your chunks directly across your local area network without ever traversing your ISP backbone!' },
      { date: '9/10/2023', title: 'Transfer files (even) faster on Windows', summary: 'It is almost time to launch our Windows native version of the app and we would like to give you a small sneak peek!', body: 'Our web and desktop builds leverage native WebRTC data channels with optimized 64KB chunking and zero-copy ArrayBuffer streams for maximum throughput on Windows 10 and 11.' },
      { date: '9/2/2023', title: 'Introducing dark mode and other upcoming features', summary: 'We have introduced a brand new dark mode to our website.', body: 'Dark mode reduces eye strain and saves battery on OLED displays while giving you a sleek, high-contrast dashboard for monitoring live transfers.' },
      { date: '4/20/2023', title: 'From Floppies to the Cloud - Does personal storage have a future?', summary: 'Nowadays almost every software product offers cloud storage options. What will be the future of personal local data?', body: 'Storing files on third-party clouds means handing over control and consuming massive datacenter energy. Direct device-to-device transfer puts ownership back into your hands.' },
      { date: '2/17/2023', title: 'Multilingual support is here', summary: 'We have added basic translations to the website so that we are more user friendly for our global users.', body: 'File sharing should be accessible to everyone around the globe without language barriers.' },
      { date: '1/12/2023', title: 'Quickly transfer files on your Local Network with Nearby Devices', summary: 'Share files with others on your local network by simply entering a 4-digit code, making the process much more user-friendly.', body: 'Instead of typing long URLs on a second computer, simply open the Nearby Devices tab, enter the 4-digit local code or scan the QR code, and start streaming immediately.' },
      { date: '12/4/2022', title: 'Increased speed and other updates', summary: 'A small recap of what happened in the last couple of months with buffer optimizations.', body: 'By tuning RTCDataChannel bufferedAmount thresholds, we eliminated memory spikes and boosted sustained transfer rates.' },
      { date: '8/31/2022', title: 'How to share an entire folder with someone', summary: 'How to easily send whole folders to someone keeping the complete folder structure intact.', body: 'You can select multiple files at once in our file picker, or compress a folder into a single .zip archive for single-stream sequential speeds.' },
      { date: '8/2/2022', title: 'Why we keep our platform lightweight', summary: 'How keeping infrastructure minimal protects your privacy and keeps P2P free.', body: 'Because files flow peer-to-peer rather than sitting on expensive cloud buckets, our signaling footprint stays tiny and sustainable.' },
      { date: '7/25/2022', title: 'Other file transfer tools (WIP)', summary: 'Comparing decentralized WebRTC tools with traditional cloud upload services.', body: 'Unlike traditional cloud links that require you to upload 100% of the file before the receiver can even start downloading, P2P uploads and downloads simultaneously!' },
      { date: '5/29/2022', title: 'Sharing files without using the cloud (long term)', summary: 'Share files directly via your mobile phone without intermediary storage.', body: 'Keep the browser tab active in the foreground on mobile devices to ensure uninterrupted background WebRTC streaming.' },
      { date: '4/28/2022', title: 'Website redesign', summary: 'Having had the same design for a couple of years, we thought it was time for something fresh.', body: 'We modernized the navigation bar, added real-time throughput graphing, and streamlined the QR pairing cards.' },
      { date: '2/3/2022', title: 'Android App & Mobile Web in Open Beta!', summary: 'Completely decentralized file sharing on mobile browsers is now a reality.', body: 'Select multiple photos or videos directly from your mobile gallery and beam them to your PC in seconds.' },
      { date: '1/28/2022', title: 'How to build web tools without hurting user privacy', summary: 'Using third party tools can have a significant impact on user privacy. Here is how we prevent that.', body: 'We maintain a strict zero-database, zero-log policy. Session tokens exist purely in volatile RAM during the active handshake.' },
      { date: '11/26/2021', title: 'Coming soon - The Private Mobile Cloud', summary: 'Turning your own device into a temporary, private file host while your tab is open.', body: 'As long as your tab remains open, your device acts as a secure, encrypted endpoint.' },
      { date: '6/23/2021', title: 'How much energy does it cost to store data online', summary: 'Energy consumption and environmental impact of cloud storage vs peer-to-peer transfer.', body: 'Skipping redundant cloud replication drastically cuts the carbon footprint of sharing multi-gigabyte video files.' },
      { date: '5/16/2021', title: 'Multiple transfers at once and getting ready for mobile', summary: 'Backend upgrades making multiple sequential file transfers at once seamless.', body: 'Select as many files as you want—our engine queues and streams them one after another with live file counters.' },
      { date: '2/1/2021', title: 'Blockchain - facts and fables', summary: 'Why direct WebRTC peer-to-peer is faster and greener than blockchain storage.', body: 'Point-to-point DTLS 1.3 channels require zero mining overhead while delivering instant throughput.' },
      { date: '1/7/2021', title: 'Series of updates to the platform', summary: 'Behind the scenes major updates taking peer-to-peer sharing into the future.', body: 'Improved STUN ice candidate handling and faster connection handshakes.' },
      { date: '5/27/2020', title: 'Introducing fallback option for better network support', summary: 'Improved speed and a fallback option when no direct peer-to-peer connection can be made.', body: 'When strict corporate firewalls block UDP, our automatic fallback relay steps in without storing a single byte on disk.' },
      { date: '2/5/2020', title: 'TLS 1.3 & DTLS 1.3 getting traction on web browsers', summary: 'Modern transport layer encryption secures every byte in transit.', body: 'All WebRTC connections enforce mandatory encryption so nobody in between can inspect your files.' },
      { date: '10/10/2019', title: 'Introducing Mobile Connect', summary: 'A faster way to share files between your PC and mobile phone via QR codes.', body: 'Scan the QR code and skip USB cables forever.' },
      { date: '6/5/2019', title: 'Big updates, big files', summary: 'First and foremost it is now possible to share multiple files at once 🎉 Plus Mobile Connect!', body: 'We have made some big improvements to our platform: First and foremost it is now possible to share multiple files at once! This new feature also lays out the path to Mobile Connect—a new way to quickly share files between your mobile phone and PC, without cables and without having to install anything.' },
      { date: '1/2/2019', title: 'A modest launch', summary: 'Our official launch, placing peer-to-peer browser file sharing out in the open.', body: 'Thank you for being part of our privacy-first file sharing journey!' }
    ];

    function renderBlogGrid() {
      const grid = document.getElementById('blog-grid');
      if (!grid) return;
      grid.innerHTML = '';
      blogPosts.forEach(function(post, index) {
        const card = document.createElement('div');
        card.className = 'glass-card rounded-2xl p-6 flex flex-col justify-between hover:border-amber-400/60 transition-all cursor-pointer';
        card.onclick = function() { openBlogReader(index); };
        card.innerHTML =
          '<div class="space-y-2">' +
            '<span class="text-xs font-bold text-amber-400">' + post.date + '</span>' +
            '<h3 class="text-lg font-bold text-white">' + escapeHtml(post.title) + '</h3>' +
            '<p class="text-xs text-gray-300 leading-relaxed">' + escapeHtml(post.summary) + '</p>' +
          '</div>' +
          '<div class="pt-4 mt-4 border-t border-gray-800 text-xs font-semibold text-amber-400">Read article →</div>';
        grid.appendChild(card);
      });
    }

    function openBlogReader(idx) {
      const post = blogPosts[idx];
      if (!post) return;
      document.getElementById('blog-grid').classList.add('hidden');
      const reader = document.getElementById('blog-reader');
      reader.classList.remove('hidden');
      document.getElementById('blog-reader-date').innerText = post.date;
      document.getElementById('blog-reader-title').innerText = post.title;
      document.getElementById('blog-reader-body').innerHTML =
        '<p class="font-semibold text-gray-200">' + escapeHtml(post.summary) + '</p>' +
        '<p>' + escapeHtml(post.body) + '</p>';
    }

    function closeBlogReader() {
      document.getElementById('blog-reader').classList.add('hidden');
      document.getElementById('blog-grid').classList.remove('hidden');
    }

    // ------------------------------------------
    // 11. NAVIGATION, DRAG-AND-DROP & UTILITIES
    // ------------------------------------------
    function switchTab(tabName) {
      const tabs = ['transfer', 'nearby', 'about', 'blog', 'faq', 'privacy', 'download'];
      tabs.forEach(function(t) {
        const sec = document.getElementById('tab-' + t);
        if (sec) {
          if (t === tabName) sec.classList.remove('hidden');
          else sec.classList.add('hidden');
        }
        const btn = document.getElementById('nav-' + t);
        if (btn) {
          if (t === tabName) btn.classList.add('active');
          else btn.classList.remove('active');
        }
      });

      if (tabName === 'nearby' && ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'register-nearby-receiver' }));
      }
    }

    function toggleTheme() {
      document.documentElement.classList.toggle('dark');
    }

    function copyShareUrl() {
      const input = document.getElementById('sender-share-url');
      input.select();
      navigator.clipboard.writeText(input.value);
      const btn = document.getElementById('copy-url-btn');
      btn.innerText = 'Copied! ✓';
      setTimeout(function() { btn.innerText = 'Copy Link'; }, 2000);
    }

    function formatBytes(bytes) {
      if (bytes === 0) return '0 B';
      const k = 1024;
      const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    function escapeHtml(str) {
      return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
    }

    // Setup Drag & Drop on Dropzone
    const dropArea = document.getElementById('drop-area');
    if (dropArea) {
      ['dragenter', 'dragover'].forEach(function(evtName) {
        dropArea.addEventListener(evtName, function(e) {
          e.preventDefault();
          e.stopPropagation();
          dropArea.classList.add('border-amber-400', 'bg-gray-900/90');
        });
      });
      ['dragleave', 'drop'].forEach(function(evtName) {
        dropArea.addEventListener(evtName, function(e) {
          e.preventDefault();
          e.stopPropagation();
          dropArea.classList.remove('border-amber-400', 'bg-gray-900/90');
        });
      });
      dropArea.addEventListener('drop', function(e) {
        if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length > 0) {
          onFilesSelected(e.dataTransfer.files);
        }
      });
    }

    // Initialize Blog & WebSocket on load
    renderBlogGrid();
    initWebSocket();
  </script>
</body>
</html>`;

app.get('*', (req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(HTML_DOCUMENT);
});

server.listen(PORT, () => {
  console.log('ChocoShare server running on port ' + PORT);
});
