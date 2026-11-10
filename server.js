const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

// Track active rooms for P2P signaling
const rooms = new Map();

wss.on('connection', (ws) => {
  let currentRoom = null;

  ws.on('message', (message) => {
    let data;
    try {
      data = JSON.parse(message);
    } catch (e) {
      return;
    }

    if (data.type === 'join-room') {
      currentRoom = data.room;
      if (!rooms.has(currentRoom)) {
        rooms.set(currentRoom, new Set());
      }
      rooms.get(currentRoom).add(ws);
      
      const peerCount = rooms.get(currentRoom).size;
      ws.send(JSON.stringify({ type: 'room-status', peers: peerCount }));
    } 
    else if (data.type === 'signal' && currentRoom) {
      const roomPeers = rooms.get(currentRoom);
      if (roomPeers) {
        roomPeers.forEach((peer) => {
          if (peer !== ws && peer.readyState === ws.OPEN) {
            peer.send(JSON.stringify({ type: 'signal', payload: data.payload }));
          }
        });
      }
    }
  });

  ws.on('close', () => {
    if (currentRoom && rooms.has(currentRoom)) {
      rooms.get(currentRoom).delete(ws);
      if (rooms.get(currentRoom).size === 0) {
        rooms.delete(currentRoom);
      }
    }
  });
});

// Serve the fully embedded frontend UI
app.get('*', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>NexusShare - P2P Secure Transfer</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          colors: {
            metal: {
              900: '#0b0f19',
              800: '#111827',
              700: '#1f2937',
              600: '#374151',
              500: '#4b5563',
              400: '#9ca3af',
              300: '#d1d5db',
            }
          }
        }
      }
    }
  </script>
  <style>
    body {
      background: radial-gradient(circle at center, #111827 0%, #030712 100%);
      color: #f3f4f6;
      font-family: ui-sans-serif, system-ui, -apple-system, sans-serif;
    }
    .metallic-card {
      background: linear-gradient(135deg, rgba(31, 41, 55, 0.7) 0%, rgba(17, 24, 39, 0.9) 100%);
      backdrop-filter: blur(12px);
      border: 1px solid rgba(75, 85, 99, 0.3);
      box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5), 0 10px 10px -5px rgba(0, 0, 0, 0.4);
    }
  </style>
</head>
<body class="min-h-screen flex flex-col justify-between p-4 md:p-8">

  <!-- Header -->
  <header class="max-w-4xl mx-auto w-full flex justify-between items-center py-4 border-b border-gray-800">
    <div class="flex items-center space-x-3">
      <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-gray-700 to-gray-500 flex items-center justify-center border border-gray-600 shadow-inner">
        <svg class="w-6 h-6 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4"></path></svg>
      </div>
      <div>
        <h1 class="text-xl font-bold tracking-wider text-gray-100 uppercase">Nexus<span class="text-emerald-400">Share</span></h1>
        <p class="text-xs text-gray-400">Direct Browser-to-Browser P2P Transfer</p>
      </div>
    </div>
    <div class="flex items-center space-x-2">
      <span class="inline-block w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></span>
      <span id="connection-status" class="text-xs font-medium text-emerald-400 tracking-wide uppercase">Ready</span>
    </div>
  </header>

  <!-- Main Content Area -->
  <main class="max-w-3xl mx-auto w-full my-8 space-y-6">
    
    <!-- Dropzone / Controls -->
    <div class="metallic-card rounded-2xl p-6 md:p-10 text-center relative overflow-hidden transition-all">
      <div id="drop-zone" class="border-2 border-dashed border-gray-600 hover:border-emerald-500 rounded-xl p-8 transition-all cursor-pointer flex flex-col items-center justify-center space-y-4">
        <div class="p-4 bg-gray-800/80 rounded-full border border-gray-700 text-emerald-400">
          <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"></path></svg>
        </div>
        <div>
          <p class="text-lg font-semibold text-gray-200">Drag & drop any file here</p>
          <p class="text-sm text-gray-400 mt-1">No size limits. Streams directly from your device.</p>
        </div>
        <input type="file" id="file-input" class="hidden">
        <button onclick="document.getElementById('file-input').click()" class="px-6 py-2.5 bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white font-medium text-sm rounded-lg shadow-lg transition-all transform hover:-translate-y-0.5">
          Browse File
        </button>
      </div>

      <!-- Active Transfer Dashboard -->
      <div id="dashboard" class="hidden space-y-6 text-left">
        <div class="flex justify-between items-center bg-gray-900/60 p-4 rounded-xl border border-gray-800">
          <div>
            <p id="file-name" class="text-sm font-bold text-gray-200 truncate max-w-xs md:max-w-md">file.zip</p>
            <p id="file-size" class="text-xs text-gray-400 mt-0.5">0 MB</p>
          </div>
          <button id="stop-btn" onclick="abortTransfer()" class="px-4 py-2 bg-red-600/80 hover:bg-red-600 text-white text-xs font-semibold rounded-lg transition-all shadow">
            Stop & Restart
          </button>
        </div>

        <!-- Live Metrics Cards -->
        <div class="grid grid-cols-3 gap-4">
          <div class="bg-gray-900/60 p-4 rounded-xl border border-gray-800">
            <p class="text-xs text-gray-400 uppercase tracking-wider">Speed</p>
            <p id="speed-val" class="text-lg font-bold text-emerald-400 mt-1">0.00 MB/s</p>
          </div>
          <div class="bg-gray-900/60 p-4 rounded-xl border border-gray-800">
            <p class="text-xs text-gray-400 uppercase tracking-wider">ETA</p>
            <p id="eta-val" class="text-lg font-bold text-gray-200 mt-1">-- s</p>
          </div>
          <div class="bg-gray-900/60 p-4 rounded-xl border border-gray-800">
            <p class="text-xs text-gray-400 uppercase tracking-wider">Progress</p>
            <p id="percent-val" class="text-lg font-bold text-teal-400 mt-1">0%</p>
          </div>
        </div>

        <!-- Progress Bar -->
        <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden border border-gray-700">
          <div id="progress-bar" class="bg-gradient-to-r from-emerald-500 to-teal-400 h-full w-0 transition-all duration-150"></div>
        </div>
      </div>
    </div>

    <!-- Link Sharing Box -->
    <div class="metallic-card rounded-2xl p-6 flex flex-col space-y-3">
      <label class="text-xs font-semibold text-gray-400 uppercase tracking-wider">Secure Room Link</label>
      <div class="flex space-x-2">
        <input type="text" id="share-link" readonly class="w-full bg-gray-900/80 border border-gray-700 rounded-xl px-4 py-2.5 text-sm text-gray-300 focus:outline-none">
        <button onclick="copyLink()" class="px-5 py-2.5 bg-gray-800 hover:bg-gray-700 border border-gray-600 text-gray-200 font-medium text-sm rounded-xl transition-all">
          Copy
        </button>
      </div>
    </div>

  </main>

  <!-- Footer -->
  <footer class="max-w-4xl mx-auto w-full text-center py-4 border-t border-gray-800 text-xs text-gray-500">
    Zero logs. Zero storage. Direct peer-to-peer WebRTC encryption.
  </footer>

  <script>
    const CHUNK_SIZE = 64 * 1024;
    let ws, pc, dataChannel;
    let roomId = window.location.hash.substring(1);
    
    if (!roomId) {
      roomId = Math.random().toString(36).substring(2, 10);
      window.location.hash = roomId;
    }
    document.getElementById('share-link').value = window.location.href;

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(protocol + '//' + window.location.host);

    ws.onopen = () => {
      ws.send(JSON.stringify({ type: 'join-room', room: roomId }));
    };

    ws.onmessage = async (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'signal') {
        if (!pc) setupPeerConnection(false);
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
      } else if (data.type === 'room-status') {
        if (data.peers > 1 && !pc) {
          setupPeerConnection(true);
        }
      }
    };

    const rtcConfig = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

    function setupPeerConnection(isInitiator) {
      pc = new RTCPeerConnection(rtcConfig);

      pc.onicecandidate = (event) => {
        if (event.candidate) {
          ws.send(JSON.stringify({ type: 'signal', payload: { candidate: event.candidate } }));
        }
      };

      if (isInitiator) {
        dataChannel = pc.createDataChannel('file-transfer');
        setupDataChannel();
        pc.createOffer().then(offer => pc.setLocalDescription(offer)).then(() => {
          ws.send(JSON.stringify({ type: 'signal', payload: { sdp: pc.localDescription } }));
        });
      } else {
        pc.ondatachannel = (event) => {
          dataChannel = event.channel;
          setupDataChannel();
        };
      }
      document.getElementById('connection-status').innerText = "Connected";
    }

    let incomingFileMeta = {};
    let receivedBuffers = [];
    let receivedSize = 0;

    function setupDataChannel() {
      dataChannel.onopen = () => console.log('Data channel open');
      dataChannel.onclose = () => console.log('Data channel closed');
      dataChannel.onmessage = async (event) => {
        if (typeof event.data === 'string') {
          const msg = JSON.parse(event.data);
          if (msg.type === 'meta') {
            incomingFileMeta = msg;
            receivedBuffers = [];
            receivedSize = 0;
            showDashboard(msg.name, msg.size);
          } else if (msg.type === 'abort') {
            resetUI();
          }
        } else {
          receivedBuffers.push(event.data);
          receivedSize += event.data.byteLength;
          updateProgress(receivedSize, incomingFileMeta.size);

          if (receivedSize === incomingFileMeta.size) {
            const blob = new Blob(receivedBuffers);
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = incomingFileMeta.name;
            a.click();
            resetUI();
          }
        }
      };
    }

    const fileInput = document.getElementById('file-input');
    fileInput.onchange = (e) => {
      const file = e.target.files[0];
      if (!file) return;
      if (!dataChannel || dataChannel.readyState !== 'open') {
        alert("Waiting for peer to connect via link...");
        return;
      }
      showDashboard(file.name, file.size);
      dataChannel.send(JSON.stringify({ type: 'meta', name: file.name, size: file.size }));
      sendFile(file);
    };

    let startTime, bytesSent = 0;
    function sendFile(file) {
      startTime = performance.now();
      bytesSent = 0;
      const reader = new FileReader();
      let offset = 0;

      reader.onload = (e) => {
        if (dataChannel.bufferedAmount > 16 * 1024 * 1024) {
          setTimeout(() => readNextChunk(), 50);
          return;
        }
        dataChannel.send(e.target.result);
        bytesSent += e.target.result.byteLength;
        offset += e.target.result.byteLength;
        updateProgress(bytesSent, file.size);

        if (offset < file.size) {
          readNextChunk();
        }
      };

      function readNextChunk() {
        const slice = file.slice(offset, offset + CHUNK_SIZE);
        reader.readAsArrayBuffer(slice);
      }
      readNextChunk();
    }

    function updateProgress(current, total) {
      const elapsed = (performance.now() - startTime) / 1000;
      const speed = elapsed > 0 ? (current / elapsed) / (1024 * 1024) : 0;
      const remaining = total - current;
      const eta = speed > 0 ? Math.round(remaining / (speed * 1024 * 1024)) : 0;
      const percent = Math.round((current / total) * 100);

      document.getElementById('speed-val').innerText = speed.toFixed(2) + ' MB/s';
      document.getElementById('eta-val').innerText = eta + 's';
      document.getElementById('percent-val').innerText = percent + '%';
      document.getElementById('progress-bar').style.width = percent + '%';
    }

    function showDashboard(name, size) {
      document.getElementById('drop-zone').classList.add('hidden');
      document.getElementById('dashboard').classList.remove('hidden');
      document.getElementById('file-name').innerText = name;
      document.getElementById('file-size').innerText = (size / (1024*1024)).toFixed(2) + ' MB';
    }

    function abortTransfer() {
      if (dataChannel && dataChannel.readyState === 'open') {
        dataChannel.send(JSON.stringify({ type: 'abort' }));
      }
      resetUI();
    }

    function resetUI() {
      document.getElementById('drop-zone').classList.remove('hidden');
      document.getElementById('dashboard').classList.add('hidden');
      document.getElementById('progress-bar').style.width = '0%';
    }

    function copyLink() {
      const copyText = document.getElementById('share-link');
      copyText.select();
      navigator.clipboard.writeText(copyText.value);
      alert('Room link copied to clipboard!');
    }
  </script>
</body>
</html>`);
});

server.listen(PORT, () => {
  console.log('NexusShare server running on port ' + PORT);
});
