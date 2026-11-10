const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

// Track active transfer sessions by unique file ID/token
const activeSessions = new Map();

wss.on('connection', (ws) => {
  let currentSession = null;
  let role = null; // 'sender' or 'receiver'

  ws.on('message', (message) => {
    let data;
    try {
      data = JSON.parse(message);
    } catch (e) {
      return;
    }

    if (data.type === 'create-session') {
      currentSession = data.sessionId;
      role = 'sender';
      activeSessions.set(currentSession, { sender: ws, receivers: [] });
    }
    else if (data.type === 'join-session') {
      currentSession = data.sessionId;
      role = 'receiver';
      const session = activeSessions.get(currentSession);
      if (session) {
        session.receivers.push(ws);
        // Notify sender that a receiver has arrived
        if (session.sender.readyState === ws.OPEN) {
          session.sender.send(JSON.stringify({ type: 'peer-joined' }));
        }
      } else {
        ws.send(JSON.stringify({ type: 'error', message: 'Session expired or not found.' }));
      }
    }
    else if (data.type === 'signal') {
      const session = activeSessions.get(currentSession);
      if (!session) return;

      if (role === 'sender') {
        // Forward signal to all receivers
        session.receivers.forEach(r => {
          if (r.readyState === ws.OPEN) r.send(JSON.stringify({ type: 'signal', payload: data.payload }));
        });
      } else {
        // Forward signal to sender
        if (session.sender.readyState === ws.OPEN) {
          session.sender.send(JSON.stringify({ type: 'signal', payload: data.payload }));
        }
      }
    }
  });

  ws.on('close', () => {
    if (currentSession && activeSessions.has(currentSession)) {
      const session = activeSessions.get(currentSession);
      if (role === 'sender') {
        activeSessions.delete(currentSession);
      }
    }
  });
});

// Serve frontend UI with complete ToffeeShare style & QR support
app.get('*', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>NexusShare - Secure P2P File Sharing</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.js"></script>
  <script src="https://unpkg.com/html5-qrcode" type="text/javascript"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          colors: {
            metal: { 900: '#0b0f19', 800: '#111827', 700: '#1f2937', 600: '#374151', 500: '#4b5563', 400: '#9ca3af' }
          }
        }
      }
    }
  </script>
  <style>
    body { background: radial-gradient(circle at center, #111827 0%, #030712 100%); color: #f3f4f6; font-family: ui-sans-serif, system-ui, sans-serif; }
    .metallic-card { background: linear-gradient(135deg, rgba(31, 41, 55, 0.7) 0%, rgba(17, 24, 39, 0.9) 100%); backdrop-filter: blur(12px); border: 1px solid rgba(75, 85, 99, 0.3); box-shadow: 0 20px 25px -5px rgba(0,0,0,0.5); }
  </style>
</head>
<body class="min-h-screen flex flex-col justify-between p-4 md:p-8">

  <!-- Header -->
  <header class="max-w-4xl mx-auto w-full flex justify-between items-center py-4 border-b border-gray-800">
    <div class="flex items-center space-x-3 cursor-pointer" onclick="location.reload()">
      <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-gray-700 to-gray-500 flex items-center justify-center border border-gray-600">
        <svg class="w-6 h-6 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4"></path></svg>
      </div>
      <div>
        <h1 class="text-xl font-bold tracking-wider text-gray-100 uppercase">Nexus<span class="text-emerald-400">Share</span></h1>
        <p class="text-xs text-gray-400">Direct P2P Encrypted Transfer</p>
      </div>
    </div>
    <div class="flex items-center space-x-2">
      <span class="inline-block w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></span>
      <span id="global-status" class="text-xs font-medium text-emerald-400 uppercase">Online</span>
    </div>
  </header>

  <!-- Main Dynamic Container -->
  <main class="max-w-3xl mx-auto w-full my-8 space-y-6">

    <!-- VIEW 1: Initial Selection (Upload or Near-by Devices) -->
    <div id="view-home" class="space-y-6">
      <div class="metallic-card rounded-2xl p-8 text-center space-y-6">
        <h2 class="text-2xl font-bold text-gray-100">Send files instantly, peer-to-peer</h2>
        <p class="text-sm text-gray-400">No cloud storage. No limits. Direct device-to-device streaming.</p>
        
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4 pt-4">
          <!-- Upload Button Card -->
          <div onclick="document.getElementById('file-input').click()" class="border-2 border-dashed border-gray-600 hover:border-emerald-500 rounded-2xl p-6 cursor-pointer transition-all flex flex-col items-center justify-center space-y-3 bg-gray-900/40 hover:bg-gray-800/40">
            <div class="p-3 bg-emerald-500/10 rounded-full text-emerald-400">
              <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"/></svg>
            </div>
            <span class="font-semibold text-gray-200">Upload Files</span>
            <span class="text-xs text-gray-400">Select single or multiple files</span>
          </div>
          <input type="file" id="file-input" multiple class="hidden" onchange="handleFileSelection(this.files)">

          <!-- Near-by Devices / Scan QR Card -->
          <div onclick="openQRScanner()" class="border-2 border-dashed border-gray-600 hover:border-teal-500 rounded-2xl p-6 cursor-pointer transition-all flex flex-col items-center justify-center space-y-3 bg-gray-900/40 hover:bg-gray-800/40">
            <div class="p-3 bg-teal-500/10 rounded-full text-teal-400">
              <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm12 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z"/></svg>
            </div>
            <span class="font-semibold text-gray-200">Near-by Devices (Scan QR)</span>
            <span class="text-xs text-gray-400">Scan code using camera</span>
          </div>
        </div>
      </div>
    </div>

    <!-- VIEW 2: QR Scanner Modal/Box -->
    <div id="view-scanner" class="hidden metallic-card rounded-2xl p-6 text-center space-y-4">
      <h3 class="text-lg font-bold text-gray-200">Scan Receiver / Sender QR Code</h3>
      <div id="reader" class="w-full max-w-sm mx-auto overflow-hidden rounded-xl border border-gray-700"></div>
      <button onclick="closeQRScanner()" class="px-5 py-2 bg-gray-700 hover:bg-gray-600 text-xs font-semibold rounded-lg text-white">Cancel</button>
    </div>

    <!-- VIEW 3: Sender Share View (Link & QR Code generated AFTER file upload selection) -->
    <div id="view-sender" class="hidden metallic-card rounded-2xl p-6 md:p-8 space-y-6">
      <div class="flex justify-between items-center border-b border-gray-800 pb-4">
        <div>
          <h3 class="text-lg font-bold text-gray-100">File Ready to Share</h3>
          <p id="selected-files-summary" class="text-xs text-emerald-400 mt-1">0 files selected</p>
        </div>
        <button onclick="resetApp()" class="px-3 py-1.5 bg-red-600/80 hover:bg-red-600 text-xs font-semibold rounded-lg text-white">Cancel / Restart</button>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-6 items-center">
        <div class="space-y-4">
          <label class="text-xs font-semibold text-gray-400 uppercase tracking-wider">Share Link</label>
          <div class="flex space-x-2">
            <input type="text" id="share-link-input" readonly class="w-full bg-gray-900/80 border border-gray-700 rounded-xl px-4 py-2.5 text-sm text-gray-300 focus:outline-none">
            <button onclick="copyShareLink()" class="px-4 py-2.5 bg-gray-800 hover:bg-gray-700 border border-gray-600 text-gray-200 font-medium text-sm rounded-xl">Copy</button>
          </div>
          <p class="text-xs text-gray-400">Keep this browser tab open until the transfer completes.</p>
        </div>
        <div class="flex flex-col items-center justify-center p-4 bg-gray-900/60 rounded-xl border border-gray-800">
          <div id="qrcode-container" class="bg-white p-3 rounded-lg shadow"></div>
          <span class="text-xs text-gray-400 mt-2">Scan with Near-by device camera</span>
        </div>
      </div>

      <!-- Transfer Dashboard for Sender -->
      <div id="sender-dashboard" class="hidden space-y-4 pt-4 border-t border-gray-800">
        <div class="grid grid-cols-3 gap-4 text-center">
          <div class="bg-gray-900/60 p-3 rounded-xl border border-gray-800">
            <p class="text-xs text-gray-400 uppercase">Speed</p>
            <p id="sender-speed" class="text-base font-bold text-emerald-400 mt-1">0 MB/s</p>
          </div>
          <div class="bg-gray-900/60 p-3 rounded-xl border border-gray-800">
            <p class="text-xs text-gray-400 uppercase">ETA</p>
            <p id="sender-eta" class="text-base font-bold text-gray-200 mt-1">-- s</p>
          </div>
          <div class="bg-gray-900/60 p-3 rounded-xl border border-gray-800">
            <p class="text-xs text-gray-400 uppercase">Progress</p>
            <p id="sender-percent" class="text-base font-bold text-teal-400 mt-1">0%</p>
          </div>
        </div>
        <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden border border-gray-700">
          <div id="sender-progress-bar" class="bg-gradient-to-r from-emerald-500 to-teal-400 h-full w-0 transition-all duration-150"></div>
        </div>
      </div>
    </div>

    <!-- VIEW 4: Receiver View (Triggered when opening a specific file link) -->
    <div id="view-receiver" class="hidden metallic-card rounded-2xl p-6 md:p-8 space-y-6 text-center">
      <div class="space-y-2">
        <div class="w-12 h-12 bg-emerald-500/10 rounded-full flex items-center justify-center mx-auto text-emerald-400">
          <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/></svg>
        </div>
        <h3 id="receiver-file-name" class="text-xl font-bold text-gray-100">Incoming File Name</h3>
        <p id="receiver-file-size" class="text-sm text-gray-400">0 MB</p>
      </div>

      <button id="download-btn" onclick="startDownload()" class="px-8 py-3 bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white font-semibold text-sm rounded-xl shadow-lg transition-all transform hover:-translate-y-0.5">
        Download File
      </button>

      <!-- Transfer Dashboard for Receiver -->
      <div id="receiver-dashboard" class="hidden space-y-4 text-left pt-4 border-t border-gray-800">
        <div class="grid grid-cols-3 gap-4 text-center">
          <div class="bg-gray-900/60 p-3 rounded-xl border border-gray-800">
            <p class="text-xs text-gray-400 uppercase">Speed</p>
            <p id="receiver-speed" class="text-base font-bold text-emerald-400 mt-1">0 MB/s</p>
          </div>
          <div class="bg-gray-900/60 p-3 rounded-xl border border-gray-800">
            <p class="text-xs text-gray-400 uppercase">ETA</p>
            <p id="receiver-eta" class="text-base font-bold text-gray-200 mt-1">-- s</p>
          </div>
          <div class="bg-gray-900/60 p-3 rounded-xl border border-gray-800">
            <p class="text-xs text-gray-400 uppercase">Progress</p>
            <p id="receiver-percent" class="text-base font-bold text-teal-400 mt-1">0%</p>
          </div>
        </div>
        <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden border border-gray-700">
          <div id="receiver-progress-bar" class="bg-gradient-to-r from-emerald-500 to-teal-400 h-full w-0 transition-all duration-150"></div>
        </div>
      </div>
    </div>

  </main>

  <footer class="max-w-4xl mx-auto w-full text-center py-4 border-t border-gray-800 text-xs text-gray-500">
    Zero logs. Direct browser-to-browser P2P WebRTC sharing.
  </footer>

  <script>
    const CHUNK_SIZE = 64 * 1024;
    let ws, pc, dataChannel;
    let selectedFiles = [];
    let sessionId = new URLSearchParams(window.location.search).get('file');

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(protocol + '//' + window.location.host);

    ws.onopen = () => {
      if (sessionId) {
        // Receiver mode
        document.getElementById('view-home').classList.add('hidden');
        document.getElementById('view-receiver').classList.remove('hidden');
        ws.send(JSON.stringify({ type: 'join-session', sessionId: sessionId }));
      }
    };

    ws.onmessage = async (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'peer-joined') {
        document.getElementById('sender-dashboard').classList.remove('hidden');
        setupSenderConnection();
      } else if (data.type === 'signal') {
        if (!pc && !sessionId) setupSenderConnection();
        if (!pc && sessionId) setupReceiverConnection();

        if (data.payload.sdp) {
          await pc.setRemoteDescription(new RTCSessionDescription(data.payload.sdp));
          if (data.payload.sdp.type === 'offer') {
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            ws.send(JSON.stringify({ type: 'signal', payload: { sdp: pc.localDescription } }));
          }
        } else if (data.payload.candidate) {
          await pc.addIceCandidate(new RTCIceCandidate(data.payload.candidate));
        }
      } else if (data.type === 'error') {
        alert(data.message);
        resetApp();
      }
    };

    const rtcConfig = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

    // --- SENDER FLOW ---
    function handleFileSelection(files) {
      if (!files || files.length === 0) return;
      selectedFiles = Array.from(files);
      sessionId = Math.random().toString(36).substring(2, 12);

      ws.send(JSON.stringify({ type: 'create-session', sessionId: sessionId }));

      document.getElementById('view-home').classList.add('hidden');
      document.getElementById('view-sender').classList.remove('hidden');

      const summaryText = selectedFiles.length === 1 ? selectedFiles[0].name : \`\${selectedFiles.length} files selected\`;
      document.getElementById('selected-files-summary').innerText = summaryText;

      const shareUrl = window.location.origin + window.location.pathname + '?file=' + sessionId;
      document.getElementById('share-link-input').value = shareUrl;

      // Render QR Code
      const qrContainer = document.getElementById('qrcode-container');
      qrContainer.innerHTML = '';
      try {
        const qr = qrcode(0, 'M');
        qr.addData(shareUrl);
        qr.make();
        qrContainer.innerHTML = qr.createSvgTag({ scalable: true });
      } catch(e) {
        qrContainer.innerText = "QR Generation Failed";
      }
    }

    function setupSenderConnection() {
      pc = new RTCPeerConnection(rtcConfig);
      pc.onicecandidate = e => {
        if (e.candidate) ws.send(JSON.stringify({ type: 'signal', payload: { candidate: e.candidate } }));
      };

      dataChannel = pc.createDataChannel('file-transfer');
      setupDataChannelSender();

      pc.createOffer().then(o => pc.setLocalDescription(o)).then(() => {
        ws.send(JSON.stringify({ type: 'signal', payload: { sdp: pc.localDescription } }));
      });
    }

    function setupDataChannelSender() {
      dataChannel.onopen = async () => {
        console.log('Data channel open, sending files...');
        for (let file of selectedFiles) {
          // Send file metadata first
          dataChannel.send(JSON.stringify({ type: 'meta', name: file.name, size: file.size, type: file.type }));
          await sendFileChunks(file);
        }
      };
    }

    function sendFileChunks(file) {
      return new Promise((resolve) => {
        const startTime = performance.now();
        let bytesSent = 0;
        const reader = new FileReader();
        let offset = 0;

        reader.onload = (e) => {
          if (dataChannel.bufferedAmount > 16 * 1024 * 1024) {
            setTimeout(() => readNext(), 50);
            return;
          }
          dataChannel.send(e.target.result);
          bytesSent += e.target.result.byteLength;
          offset += e.target.result.byteLength;
          updateSenderProgress(bytesSent, file.size, startTime);

          if (offset < file.size) {
            readNext();
          } else {
            resolve();
          }
        };

        function readNext() {
          const slice = file.slice(offset, offset + CHUNK_SIZE);
          reader.readAsArrayBuffer(slice);
        }
        readNext();
      });
    }

    function updateSenderProgress(current, total, startTime) {
      const elapsed = (performance.now() - startTime) / 1000;
      const speed = elapsed > 0 ? (current / elapsed) / (1024 * 1024) : 0;
      const remaining = total - current;
      const eta = speed > 0 ? Math.round(remaining / (speed * 1024 * 1024)) : 0;
      const percent = Math.round((current / total) * 100);

      document.getElementById('sender-speed').innerText = speed.toFixed(2) + ' MB/s';
      document.getElementById('sender-eta').innerText = eta + 's';
      document.getElementById('sender-percent').innerText = percent + '%';
      document.getElementById('sender-progress-bar').style.width = percent + '%';
    }

    // --- RECEIVER FLOW ---
    function setupReceiverConnection() {
      pc = new RTCPeerConnection(rtcConfig);
      pc.onicecandidate = e => {
        if (e.candidate) ws.send(JSON.stringify({ type: 'signal', payload: { candidate: e.candidate } }));
      };

      pc.ondatachannel = e => {
        dataChannel = e.channel;
        setupDataChannelReceiver();
      };
    }

    function startDownload() {
      document.getElementById('download-btn').classList.add('hidden');
      document.getElementById('receiver-dashboard').classList.remove('hidden');
      setupReceiverConnection();
      // Send signal to start WebRTC offer/answer handshake
      pc.createAnswer().catch(() => {});
    }

    let incomingMeta = null;
    let receivedBuffers = [];
    let receivedSize = 0;
    let receiverStartTime = 0;

    function setupDataChannelReceiver() {
      dataChannel.onopen = () => { receiverStartTime = performance.now(); };
      dataChannel.onmessage = async (event) => {
        if (typeof event.data === 'string') {
          const msg = JSON.parse(event.data);
          if (msg.type === 'meta') {
            incomingMeta = msg;
            receivedBuffers = [];
            receivedSize = 0;
            document.getElementById('receiver-file-name').innerText = msg.name;
            document.getElementById('receiver-file-size').innerText = (msg.size / (1024*1024)).toFixed(2) + ' MB';
          }
        } else {
          receivedBuffers.push(event.data);
          receivedSize += event.data.byteLength;
          updateReceiverProgress(receivedSize, incomingMeta.size, receiverStartTime);

          if (receivedSize === incomingMeta.size) {
            const blob = new Blob(receivedBuffers);
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = incomingMeta.name;
            a.click();
          }
        }
      };
    }

    function updateReceiverProgress(current, total, startTime) {
      const elapsed = (performance.now() - startTime) / 1000;
      const speed = elapsed > 0 ? (current / elapsed) / (1024 * 1024) : 0;
      const remaining = total - current;
      const eta = speed > 0 ? Math.round(remaining / (speed * 1024 * 1024)) : 0;
      const percent = Math.round((current / total) * 100);

      document.getElementById('receiver-speed').innerText = speed.toFixed(2) + ' MB/s';
      document.getElementById('receiver-eta').innerText = eta + 's';
      document.getElementById('receiver-percent').innerText = percent + '%';
      document.getElementById('receiver-progress-bar').style.width = percent + '%';
    }

    // --- QR SCANNER INTEGRATION ---
    let html5QrCode = null;
    function openQRScanner() {
      document.getElementById('view-home').classList.add('hidden');
      document.getElementById('view-scanner').classList.remove('hidden');
      
      html5QrCode = new Html5Qrcode("reader");
      html5QrCode.start(
        { facingMode: "environment" },
        { fps: 10, qrbox: { width: 250, height: 250 } },
        (decodedText) => {
          html5QrCode.stop().then(() => {
            window.location.href = decodedText;
          }).catch(() => {
            window.location.href = decodedText;
          });
        },
        (errorMessage) => {}
      ).catch(err => {
        alert("Camera access denied or unavailable.");
        closeQRScanner();
      });
    }

    function closeQRScanner() {
      if (html5QrCode) {
        html5QrCode.stop().catch(() => {});
      }
      document.getElementById('view-scanner').classList.add('hidden');
      document.getElementById('view-home').classList.remove('hidden');
    }

    function copyShareLink() {
      const input = document.getElementById('share-link-input');
      input.select();
      navigator.clipboard.writeText(input.value);
      alert('Link copied to clipboard!');
    }

    function resetApp() {
      window.location.href = window.location.pathname;
    }
  </script>
</body>
</html>`);
});

server.listen(PORT, () => {
  console.log('NexusShare server running on port ' + PORT);
});
