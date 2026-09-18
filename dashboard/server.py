"""
Lightweight live dashboard for the Kafka order system.

Reads dashboard_state.json (written by consumer/consumer.py after every
processed order) and serves it as a live-updating web page. Uses only the
Python standard library -- no extra pip installs, no external CDN scripts,
so it can't fail to start because of a missing dependency or a flaky
internet connection during a demo.

Run from the project root:
    python dashboard/server.py

Then open http://localhost:8000 in a browser.
"""

import http.server
import json
import os

STATE_FILE = "dashboard_state.json"
PORT = 8000

EMPTY_STATE = json.dumps({
    "total_orders_processed": 0,
    "total_price": 0,
    "running_average": 0,
    "dlq_count": 0,
    "last_updated": None,
    "recent_orders": [],
    "avg_history": []
})

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Kafka Order System - Live Dashboard</title>
<style>
  :root { color-scheme: dark; }
  body {
    background: #0f1115;
    color: #e6e6e6;
    font-family: "Segoe UI", Roboto, Arial, sans-serif;
    margin: 0;
    padding: 24px;
  }
  h1 { font-size: 20px; font-weight: 600; margin: 0 0 4px 0; }
  .subtitle { color: #8a8f98; font-size: 13px; margin-bottom: 24px; }
  .cards {
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    margin-bottom: 24px;
  }
  .card {
    background: #171a21;
    border: 1px solid #262b36;
    border-radius: 10px;
    padding: 16px 20px;
    min-width: 160px;
  }
  .card .label {
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #8a8f98;
    margin-bottom: 6px;
  }
  .card .value {
    font-size: 28px;
    font-weight: 700;
  }
  .value.green { color: #4ade80; }
  .value.red { color: #f87171; }
  canvas {
    background: #171a21;
    border: 1px solid #262b36;
    border-radius: 10px;
    display: block;
    margin-bottom: 24px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    background: #171a21;
    border: 1px solid #262b36;
    border-radius: 10px;
    overflow: hidden;
  }
  th, td {
    text-align: left;
    padding: 10px 14px;
    font-size: 14px;
    border-bottom: 1px solid #262b36;
  }
  th {
    color: #8a8f98;
    font-weight: 600;
    font-size: 12px;
    text-transform: uppercase;
  }
  tr.row-dlq { background: rgba(248, 113, 113, 0.08); }
  tr.row-ok { background: rgba(74, 222, 128, 0.04); }
  .badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.03em;
  }
  .badge.processed { background: rgba(74, 222, 128, 0.15); color: #4ade80; }
  .badge.dlq { background: rgba(248, 113, 113, 0.15); color: #f87171; }
</style>
</head>
<body>

<h1>Kafka Order System</h1>
<div class="subtitle">Live view - last updated <span id="updated">-</span></div>

<div class="cards">
  <div class="card">
    <div class="label">Running Average Price</div>
    <div class="value green" id="avg">$0.00</div>
  </div>
  <div class="card">
    <div class="label">Orders Processed</div>
    <div class="value" id="count">0</div>
  </div>
  <div class="card">
    <div class="label">Sent to DLQ</div>
    <div class="value red" id="dlq">0</div>
  </div>
</div>

<canvas id="spark" width="900" height="140"></canvas>

<table>
  <thead>
    <tr><th>Order ID</th><th>Product</th><th>Price</th><th>Status</th></tr>
  </thead>
  <tbody id="orders-body"></tbody>
</table>

<script>
async function refresh() {
  let data;
  try {
    const res = await fetch('/api/state', { cache: 'no-store' });
    data = await res.json();
  } catch (e) {
    return; // server not reachable yet, just try again next tick
  }

  document.getElementById('avg').textContent = '$' + Number(data.running_average).toFixed(2);
  document.getElementById('count').textContent = data.total_orders_processed;
  document.getElementById('dlq').textContent = data.dlq_count;
  document.getElementById('updated').textContent =
    data.last_updated ? new Date(data.last_updated).toLocaleTimeString() : 'waiting for orders...';

  const tbody = document.getElementById('orders-body');
  tbody.innerHTML = '';
  const orders = (data.recent_orders || []).slice().reverse();
  for (const o of orders) {
    const tr = document.createElement('tr');
    tr.className = o.status === 'dlq' ? 'row-dlq' : 'row-ok';
    const badgeClass = o.status === 'dlq' ? 'dlq' : 'processed';
    tr.innerHTML =
      '<td>' + o.orderId + '</td>' +
      '<td>' + o.product + '</td>' +
      '<td>$' + Number(o.price).toFixed(2) + '</td>' +
      '<td><span class="badge ' + badgeClass + '">' + o.status.toUpperCase() + '</span></td>';
    tbody.appendChild(tr);
  }

  drawSparkline(data.avg_history || []);
}

function drawSparkline(values) {
  const canvas = document.getElementById('spark');
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  if (values.length < 2) return;

  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = (max - min) || 1;

  ctx.beginPath();
  values.forEach((v, i) => {
    const x = (i / (values.length - 1)) * (w - 20) + 10;
    const y = h - 10 - ((v - min) / range) * (h - 20);
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = '#4ade80';
  ctx.lineWidth = 2;
  ctx.stroke();
}

setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>
"""


class DashboardHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self._send_bytes(HTML_PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self._send_state()
        else:
            self.send_error(404, "Not found")

    def _send_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    body = f.read().encode("utf-8")
            except Exception:
                body = EMPTY_STATE.encode("utf-8")
        else:
            body = EMPTY_STATE.encode("utf-8")

        self._send_bytes(body, "application/json", no_cache=True)

    def _send_bytes(self, body, content_type, no_cache=False):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if no_cache:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # Silence the default per-request logging so the terminal
        # doesn't fill up with a line every second from polling.
        pass


if __name__ == "__main__":
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), DashboardHandler)
    print(f"Dashboard running at http://localhost:{PORT}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard stopped.")