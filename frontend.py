# ─────────────────────────────────────────────────────────────────────────────
#  frontend.py  —  nse-algo-bot Scanner Web Dashboard (Flask + Server-Sent Events)
#
#  Browser auto-opens when main.py starts (SCANNER_MODE only).
#  Signals are pushed in real-time via SSE — no page refresh needed.
#  All signals persist in memory for the full session (day).
#  New browser tab/refresh replays all signals already fired.
# ─────────────────────────────────────────────────────────────────────────────

import json
import queue
import threading
import webbrowser
import time
from datetime import datetime
from flask import Flask, Response, jsonify

# ── State ──────────────────────────────────────────────────────────────────────
_signals: list[dict] = []          # all signals fired this session
_subscribers: list[queue.Queue] = []  # SSE client queues
_status = {"text": "Starting...", "scanning": False}
_lock   = threading.Lock()

app = Flask(__name__)
app.config["ENV"] = "production"


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return _HTML

@app.route("/api/status")
def api_status():
    with _lock:
        return jsonify(_status)

@app.route("/api/stream")
def api_stream():
    """
    Server-Sent Events endpoint.
    New client: first receives all historical signals, then live updates.
    Heartbeat comment every 25s keeps the connection alive.
    """
    def generate():
        q = queue.Queue()
        with _lock:
            _subscribers.append(q)
            existing = list(_signals)
            cur_status = dict(_status)

        try:
            # Send current status first
            yield f"data: {json.dumps({'type': 'status', **cur_status})}\n\n"
            # Replay all existing signals (so refresh shows full day)
            for sig in existing:
                yield f"data: {json.dumps(sig)}\n\n"

            while True:
                try:
                    item = q.get(timeout=25)
                    yield f"data: {json.dumps(item)}\n\n"
                except queue.Empty:
                    yield ": heartbeat\n\n"  # SSE comment — keeps connection alive
        finally:
            with _lock:
                try:
                    _subscribers.remove(q)
                except ValueError:
                    pass

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Public API called from main.py ────────────────────────────────────────────

def add_signal(signal: dict):
    """Push a new signal to all connected clients and store it."""
    with _lock:
        _signals.append(signal)
        payload = dict(signal)
        payload["type"] = "signal"
        for q in list(_subscribers):
            try:
                q.put_nowait(payload)
            except Exception:
                pass


def set_status(text: str, scanning: bool = False):
    """Update the scan status shown in the browser header."""
    with _lock:
        _status["text"]     = text
        _status["scanning"] = scanning
        payload = {"type": "status", "text": text, "scanning": scanning}
        for q in list(_subscribers):
            try:
                q.put_nowait(payload)
            except Exception:
                pass


def lan_ip() -> str:
    """
    Best-effort LAN IP of this machine (the one another PC on the same
    network/WiFi should point its browser at). Falls back to 127.0.0.1.
    """
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # No packet is actually sent — just picks the outbound interface.
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def start_server(port: int = 5001, host: str = "0.0.0.0"):
    """
    Start Flask in a background daemon thread.

    host="0.0.0.0" binds ALL network interfaces so another PC on the same
    LAN/WiFi can open the dashboard at  http://<this-PC-LAN-IP>:<port>.
    (Use host="127.0.0.1" to restrict access to this machine only.)
    """
    import logging
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)   # suppress Flask request logs

    def _run():
        app.run(host=host, port=port, debug=False, use_reloader=False)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    time.sleep(1.0)   # give Flask a moment to bind


def open_browser(port: int = 5001):
    """Open the dashboard in the default browser (on this machine)."""
    webbrowser.open(f"http://127.0.0.1:{port}")


# ── HTML template (inline) ────────────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>nse-algo-bot Scanner</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: #f5f6fa;
    color: #1e293b;
    font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, 'Helvetica Neue', sans-serif;
    min-height: 100vh;
  }

  /* ── Header ── */
  .header {
    background: #fff;
    border-bottom: 2px solid #e2e8f0;
    padding: 12px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
  }
  .header-left { display: flex; align-items: center; gap: 16px; }
  .logo { font-size: 1.2rem; font-weight: 700; color: #1d4ed8; letter-spacing: 1px; }
  .logo small { color: #64748b; font-weight: 400; font-size: 0.78rem; margin-left: 6px; letter-spacing: 0; }
  .status-pill {
    display: flex; align-items: center; gap: 6px;
    padding: 4px 10px; border-radius: 20px;
    background: #f1f5f9; border: 1px solid #e2e8f0;
  }
  .status-dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: #94a3b8; transition: background 0.3s;
  }
  .status-dot.live {
    background: #22c55e;
    box-shadow: 0 0 0 3px rgba(34,197,94,0.2);
    animation: pulse 2s infinite;
  }
  @keyframes pulse {
    0%,100% { box-shadow: 0 0 0 3px rgba(34,197,94,0.2); }
    50%      { box-shadow: 0 0 0 6px rgba(34,197,94,0.08); }
  }
  .status-text { font-size: 0.75rem; color: #64748b; font-weight: 500; }
  .header-right { text-align: right; }
  .clock { font-size: 0.98rem; font-weight: 600; color: #1e293b; font-variant-numeric: tabular-nums; }
  .date-str { font-size: 0.7rem; color: #94a3b8; margin-top: 1px; }

  /* ── Stats bar ── */
  .stats-bar {
    background: #fff;
    border-bottom: 1px solid #e2e8f0;
    padding: 8px 24px;
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
  .stat-card {
    display: flex; align-items: center; gap: 8px;
    padding: 6px 14px; border-radius: 8px;
    border: 1px solid #e2e8f0; background: #f8fafc;
  }
  .stat-label { font-size: 0.68rem; color: #64748b; font-weight: 600; text-transform: uppercase; letter-spacing: 0.7px; }
  .stat-value { font-size: 1.1rem; font-weight: 700; min-width: 22px; text-align: center; }
  .stat-value.buy   { color: #16a34a; }
  .stat-value.sell  { color: #dc2626; }
  .stat-value.total { color: #2563eb; }
  .scan-status { margin-left: auto; font-size: 0.76rem; color: #64748b; max-width: 400px; text-align: right; line-height: 1.4; }

  /* ── Toolbar ── */
  .toolbar {
    padding: 9px 24px;
    display: flex; align-items: center; gap: 8px;
    background: #f8fafc;
    border-bottom: 1px solid #e2e8f0;
    flex-wrap: wrap;
  }
  .search-wrap { position: relative; display: flex; align-items: center; }
  .search-icon {
    position: absolute; left: 9px; color: #94a3b8;
    font-size: 13px; pointer-events: none; user-select: none;
  }
  .search-box {
    padding: 6px 11px 6px 28px;
    width: 240px;
    border: 1px solid #d1d5db; border-radius: 6px;
    font-size: 0.82rem; outline: none;
    background: #fff; color: #1e293b;
    transition: border 0.15s, box-shadow 0.15s;
  }
  .search-box:focus { border-color: #2563eb; box-shadow: 0 0 0 3px rgba(37,99,235,0.1); }
  .btn {
    padding: 6px 13px; border-radius: 6px;
    font-size: 0.79rem; font-weight: 600;
    cursor: pointer; border: 1px solid transparent;
    transition: all 0.15s; white-space: nowrap;
    display: inline-flex; align-items: center; gap: 4px;
    font-family: inherit;
  }
  .btn-outline { background: #fff; border-color: #d1d5db; color: #374151; }
  .btn-outline:hover { background: #f1f5f9; border-color: #94a3b8; }
  .btn-green { background: #16a34a; color: #fff; border-color: #16a34a; }
  .btn-green:hover { background: #15803d; }

  /* ── Table ── */
  .table-wrap { padding: 14px 24px; overflow-x: auto; }
  table {
    width: 100%; border-collapse: collapse;
    background: #fff; border-radius: 10px; overflow: hidden;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06), 0 0 0 1px #e2e8f0;
  }
  thead th {
    background: #f8fafc;
    padding: 9px 14px;
    text-align: left;
    font-size: 0.68rem; font-weight: 700;
    color: #64748b; text-transform: uppercase; letter-spacing: 0.6px;
    border-bottom: 1px solid #e2e8f0;
    white-space: nowrap;
  }
  tbody tr { border-bottom: 1px solid #f1f5f9; transition: background 0.1s; }
  tbody tr[data-symbol] { animation: rowIn 0.25s ease; }
  @keyframes rowIn { from { opacity:0; background:#eff6ff; } to { opacity:1; } }
  tbody tr:hover { background: #f8fafc; }
  tbody tr:last-child { border-bottom: none; }
  tbody td {
    padding: 9px 14px; font-size: 0.83rem;
    color: #374151; vertical-align: middle; white-space: nowrap;
  }

  /* ── Direction badge ── */
  .badge {
    display: inline-block; padding: 2px 9px;
    border-radius: 20px; font-size: 0.67rem;
    font-weight: 700; letter-spacing: 0.7px;
  }
  .badge.buy  { background: #dcfce7; color: #15803d; border: 1px solid #bbf7d0; }
  .badge.sell { background: #fee2e2; color: #b91c1c; border: 1px solid #fecaca; }

  /* ── Symbol TV link ── */
  .tv-link { color: #2563eb; font-weight: 600; text-decoration: none; font-size: 0.88rem; }
  .tv-link:hover { text-decoration: underline; }

  /* ── Price cells ── */
  .time-val { color: #94a3b8; font-variant-numeric: tabular-nums; font-size: 0.77rem; }

  /* ── Empty state ── */
  .empty-state { text-align: center; padding: 56px 20px; color: #94a3b8; }
  .empty-state .icon { font-size: 2rem; margin-bottom: 10px; }
  .empty-state p { font-size: 0.86rem; line-height: 1.7; }
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <div class="header-left">
    <span class="logo">nse-algo-bot <small>NSE &bull; 4H indicator</small></span>
    <div class="status-pill">
      <span class="status-dot" id="dot"></span>
      <span class="status-text" id="status-text">Connecting&hellip;</span>
    </div>
  </div>
  <div class="header-right">
    <div class="clock" id="clock">--:--:--</div>
    <div class="date-str" id="date-str"></div>
  </div>
</div>

<!-- Stats bar -->
<div class="stats-bar">
  <div class="stat-card">
    <span class="stat-label">BUY</span>
    <span class="stat-value buy" id="cnt-buy">0</span>
  </div>
  <div class="stat-card">
    <span class="stat-label">SELL</span>
    <span class="stat-value sell" id="cnt-sell">0</span>
  </div>
  <div class="stat-card">
    <span class="stat-label">Total</span>
    <span class="stat-value total" id="cnt-total">0</span>
  </div>
  <div class="scan-status" id="scan-status">--</div>
</div>

<!-- Toolbar -->
<div class="toolbar">
  <div class="search-wrap">
    <span class="search-icon">&#128269;</span>
    <input class="search-box" id="search" type="text" placeholder="Search symbol&hellip;">
  </div>
  <button class="btn btn-outline" id="copy-btn" onclick="copyAll()">&#128203; Copy All</button>
  <button class="btn btn-green" onclick="downloadCSV()">&#8595; Download CSV</button>
</div>

<!-- Table -->
<div class="table-wrap">
  <table>
    <thead>
      <tr>
        <th>#</th>
        <th>Signal</th>
        <th>Symbol</th>
        <th>Time</th>
        <th>Chart</th>
      </tr>
    </thead>
    <tbody id="signals-body">
      <tr id="empty-row">
        <td colspan="5">
          <div class="empty-state">
            <div class="icon">&#x1F4E1;</div>
            <p>Waiting for signals&hellip;<br>
               <span style="font-size:0.77rem;color:#cbd5e1">Signals appear here in real-time as they are detected</span>
            </p>
          </div>
        </td>
      </tr>
    </tbody>
  </table>
</div>

<script>
// ── State ──────────────────────────────────────────────────────────────────────
const allSignals = [];
let buyCount = 0, sellCount = 0;

// ── Clock ───────────────────────────────────────────────────────────────────────
function updateClock() {
  const n = new Date();
  document.getElementById('clock').textContent =
    [n.getHours(), n.getMinutes(), n.getSeconds()]
      .map(v => String(v).padStart(2,'0')).join(':');
  document.getElementById('date-str').textContent =
    n.toLocaleDateString('en-IN', {weekday:'short', day:'numeric', month:'short', year:'numeric'});
}
setInterval(updateClock, 1000);
updateClock();

// ── Search ──────────────────────────────────────────────────────────────────────
document.getElementById('search').addEventListener('input', function () {
  const q = this.value.trim().toLowerCase();
  document.querySelectorAll('#signals-body tr[data-symbol]').forEach(tr => {
    tr.style.display = (!q || tr.dataset.symbol.includes(q)) ? '' : 'none';
  });
});


// ── Add row (newest at top) ───────────────────────────────────────────────────
function addRow(sig) {
  const body = document.getElementById('signals-body');
  const emptyRow = document.getElementById('empty-row');
  if (emptyRow) emptyRow.remove();

  const dir = sig.direction || '';
  const n   = allSignals.length;
  const tv  = 'https://www.tradingview.com/chart/?symbol=NSE%3A' +
              encodeURIComponent(sig.symbol || '') + '&interval=240';

  const tr = document.createElement('tr');
  tr.dataset.symbol = (sig.symbol || '').toLowerCase();
  tr.innerHTML = `
    <td style="color:#cbd5e1;font-size:0.72rem">${n}</td>
    <td><span class="badge ${dir.toLowerCase()}">${dir}</span></td>
    <td style="font-weight:600;font-size:0.88rem">${sig.symbol}</td>
    <td class="time-val">${sig.time || ''}</td>
    <td><a href="${tv}" target="_blank" rel="noopener noreferrer"
         style="display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:5px;
                background:#eff6ff;color:#2563eb;font-size:0.75rem;font-weight:600;
                text-decoration:none;border:1px solid #bfdbfe;"
         title="Open on TradingView">&#128200; Chart</a></td>`;

  body.insertBefore(tr, body.firstChild);
}

// ── Counters ─────────────────────────────────────────────────────────────────
function updateCounters(dir) {
  if (dir === 'BUY')  buyCount++;
  if (dir === 'SELL') sellCount++;
  document.getElementById('cnt-buy').textContent   = buyCount;
  document.getElementById('cnt-sell').textContent  = sellCount;
  document.getElementById('cnt-total').textContent = buyCount + sellCount;
}

// ── Copy All ──────────────────────────────────────────────────────────────────
function copyAll() {
  if (!allSignals.length) { alert('No signals to copy yet.'); return; }
  const hdr  = ['#','Signal','Symbol','Time'];
  const rows = allSignals.map((sig, i) =>
    [i + 1, sig.direction, sig.symbol, sig.time || ''].join('\\t')
  );
  const text = [hdr.join('\\t'), ...rows].join('\\n');
  const btn  = document.getElementById('copy-btn');
  const restore = () => { btn.innerHTML = '&#128203; Copy All'; };
  navigator.clipboard.writeText(text)
    .then(() => { btn.textContent = '\\u2713 Copied!'; setTimeout(restore, 1800); })
    .catch(() => {
      const ta = document.createElement('textarea');
      ta.value = text; document.body.appendChild(ta); ta.select();
      document.execCommand('copy'); ta.remove();
      btn.textContent = '\\u2713 Copied!'; setTimeout(restore, 1800);
    });
}

// ── Download CSV ──────────────────────────────────────────────────────────────
function downloadCSV() {
  if (!allSignals.length) { alert('No signals to download yet.'); return; }
  const hdr  = ['#','Signal','Symbol','Time','TradingView'];
  const rows = allSignals.map((sig, i) => {
    const tv = 'https://www.tradingview.com/chart/?symbol=NSE%3A' + (sig.symbol || '') + '&interval=240';
    return [i + 1, sig.direction, sig.symbol, sig.time || '', tv]
      .map(v => '"' + String(v).replace(/"/g, '""') + '"').join(',');
  });
  const csv  = '\\uFEFF' + [hdr.join(','), ...rows].join('\\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const a    = document.createElement('a');
  a.href     = URL.createObjectURL(blob);
  const d    = new Date();
  a.download = 'signals_' + d.getFullYear() +
    String(d.getMonth() + 1).padStart(2, '0') +
    String(d.getDate()).padStart(2, '0') + '.csv';
  a.click();
  URL.revokeObjectURL(a.href);
}

// ── SSE connection ────────────────────────────────────────────────────────────
const dot        = document.getElementById('dot');
const statusText = document.getElementById('status-text');
const scanStatus = document.getElementById('scan-status');

function resetState() {
  allSignals.length = 0;
  buyCount = sellCount = 0;
  document.getElementById('cnt-buy').textContent   = '0';
  document.getElementById('cnt-sell').textContent  = '0';
  document.getElementById('cnt-total').textContent = '0';
  const body = document.getElementById('signals-body');
  body.innerHTML = '<tr id="empty-row"><td colspan="5">' +
    '<div class="empty-state"><div class="icon">&#x1F4E1;</div>' +
    '<p>Reconnecting&hellip;</p></div></td></tr>';
}

function connectSSE() {
  const es = new EventSource('/api/stream');

  es.onopen = () => {
    dot.classList.add('live');
    statusText.textContent = 'Connected';
  };

  es.onmessage = (e) => {
    const data = JSON.parse(e.data);

    if (data.type === 'status') {
      scanStatus.textContent = data.text || '';
      if (data.scanning) {
        dot.classList.add('live');
        statusText.textContent = 'Scanning live';
      } else {
        dot.classList.remove('live');
        const t = (data.text || '').toLowerCase();
        statusText.textContent = t.includes('complet') ? 'Session complete' : 'Idle';
      }
      return;
    }

    if (data.type === 'signal' || data.direction) {
      allSignals.push(data);
      addRow(data);
      updateCounters(data.direction);
    }
  };

  es.onerror = () => {
    dot.classList.remove('live');
    statusText.textContent = 'Reconnecting…';
    es.close();
    resetState();
    setTimeout(connectSSE, 3000);
  };
}

connectSSE();
</script>
</body>
</html>
"""
