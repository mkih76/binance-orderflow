#!/usr/bin/env python3
"""
订单流 Web 可视化仪表盘
========================
Flask + Socket.IO 实时推送
浏览器访问: http://服务器IP:8765
"""

import json
import time
import threading
import sys
import os
from collections import deque
from datetime import datetime, timezone

# SOCKS5 代理
import socks
import socket as sock_module
socks.set_default_proxy(socks.SOCKS5, "127.0.0.1", 1080)
sock_module.socket = socks.socksocket

import websocket
from flask import Flask, render_template_string, jsonify
from flask_socketio import SocketIO

sys.path.insert(0, "/opt/binance-testnet")
from orderflow import (
    FootprintChart, DeltaTracker, VolumeProfile,
    ImbalanceDetector, AbsorptionDetector, ExhaustionDetector,
    IcebergDetector, SpeedOfTape, OrderFlowSignalEngine
)
from trader import get_balance, get_positions, place_order, get_base_url, get_current_mode

app = Flask(__name__)
app.config['SECRET_KEY'] = 'orderflow-secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ==================== 数据层 ====================

class DashboardData:
    def __init__(self):
        self.trades = deque(maxlen=5000)
        self.depth = {"bids": [], "asks": []}
        self.mark_price = None
        self.trade_count = 0
        self.msg_per_sec = 0
        self.avg_latency = 0
        self._latencies = deque(maxlen=100)
        self._msg_counter = 0
        self._counter_reset = time.time()
        
        # 分析引擎
        self.engine = OrderFlowSignalEngine(tick_size=1.0)
        self.signals = []
        self.consensus = "neutral"
        self.confidence = 0
        
        # 历史数据（用于图表）
        self.cvd_history = deque(maxlen=200)
        self.price_history = deque(maxlen=200)
        self.delta_history = deque(maxlen=200)
        self.speed_history = deque(maxlen=200)
        self.signal_log = deque(maxlen=50)
        
        # 交易状态
        self.balance = {}
        self.positions = []
        self.open_order = None
    
    def add_trade(self, data):
        now = time.time()
        trade = {
            "p": data["p"], "q": data["q"],
            "T": data["T"], "m": data["m"],
        }
        self.trades.append(trade)
        self.trade_count += 1
        
        latency = (now - data["T"] / 1000) * 1000
        if 0 < latency < 10000:
            self._latencies.append(latency)
            self.avg_latency = sum(self._latencies) / len(self._latencies)
        
        self._msg_counter += 1
        if now - self._counter_reset >= 1.0:
            self.msg_per_sec = self._msg_counter
            self._msg_counter = 0
            self._counter_reset = now
    
    def update_depth(self, data):
        bids = data.get("bids", data.get("b", []))
        asks = data.get("asks", data.get("a", []))
        self.depth["bids"] = [(float(p), float(q)) for p, q in bids[:10]]
        self.depth["asks"] = [(float(p), float(q)) for p, q in asks[:10]]
    
    def analyze(self):
        trades = list(self.trades)[-2000:]
        if len(trades) < 50:
            return
        
        self.engine.feed(trades)
        self.signals = self.engine.analyze(trades)
        self.consensus, self.confidence = self.engine.get_consensus()
        
        # 记录历史
        price = float(trades[-1]["p"])
        cvd = self.engine.delta.cvd
        delta = self.engine.delta.current_delta
        speed = self.engine.speed
        
        ts = time.time() * 1000
        self.price_history.append({"t": ts, "v": price})
        self.cvd_history.append({"t": ts, "v": cvd})
        self.delta_history.append({"t": ts, "v": delta})
        
        if speed.speed_history:
            self.speed_history.append({"t": ts, "v": speed.speed_history[-1]["speed"]})
        
        # 信号日志
        for s in self.signals:
            entry = {
                "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
                "source": s.get("source", "?"),
                "type": s.get("type", "?"),
                "bias": s.get("bias", "neutral"),
                "detail": s.get("reason", ""),
            }
            if not self.signal_log or self.signal_log[-1] != entry:
                self.signal_log.append(entry)
    
    def get_snapshot(self):
        bid, ask = (0, 0)
        if self.depth["bids"]:
            bid = self.depth["bids"][0][0]
        if self.depth["asks"]:
            ask = self.depth["asks"][0][0]
        
        vah, val, poc = self.engine.volume_profile.get_value_area()
        
        # 持仓量历史（简化）
        oi_data = []
        try:
            import requests
            r = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                           params={"symbol": "BTCUSDT", "period": "1h", "limit": 12},
                           proxies={"http": "socks5://127.0.0.1:1080", "https": "socks5://127.0.0.1:1080"},
                           timeout=10)
            if r.status_code == 200:
                for item in r.json():
                    oi_data.append({
                        "t": item["timestamp"],
                        "oi": float(item["sumOpenInterest"]),
                        "val": float(item["sumOpenInterestValue"]),
                    })
        except:
            pass
        
        # 多空比
        ls_data = {"long_pct": 0, "short_pct": 0, "ratio": 0}
        try:
            import requests
            r = requests.get("https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
                           params={"symbol": "BTCUSDT", "period": "5m", "limit": 1},
                           proxies={"http": "socks5://127.0.0.1:1080", "https": "socks5://127.0.0.1:1080"},
                           timeout=10)
            if r.status_code == 200 and r.json():
                item = r.json()[0]
                ls_data = {
                    "long_pct": float(item["longAccount"]) * 100,
                    "short_pct": float(item["shortAccount"]) * 100,
                    "ratio": float(item["longShortRatio"]),
                }
        except:
            pass
        
        return {
            "price": bid if bid else (ask if ask else 0),
            "bid": bid,
            "ask": ask,
            "spread": ask - bid if bid and ask else 0,
            "cvd": self.engine.delta.cvd,
            "delta": self.engine.delta.current_delta,
            "poc": poc,
            "vah": vah,
            "val": val,
            "trades": self.trade_count,
            "msg_per_sec": self.msg_per_sec,
            "latency": round(self.avg_latency),
            "consensus": self.consensus,
            "confidence": round(self.confidence * 100),
            "signals": [
                {
                    "source": s.get("source", "?"),
                    "type": s.get("type", "?"),
                    "bias": s.get("bias", "neutral"),
                    "strength": s.get("strength", 0),
                }
                for s in self.signals
            ],
            "signal_log": list(self.signal_log)[-10:],
            "cvd_history": list(self.cvd_history)[-100:],
            "price_history": list(self.price_history)[-100:],
            "delta_history": list(self.delta_history)[-100:],
            "speed_history": list(self.speed_history)[-100:],
            "oi_data": oi_data,
            "ls_data": ls_data,
            "volume_profile": self.engine.volume_profile.profile if self.engine.volume_profile.profile else {},
            "timestamp": time.time() * 1000,
        }

data = DashboardData()

# ==================== WebSocket 连接 ====================

def ws_worker():
    """后台 WebSocket 工作线程"""
    url = "wss://fstream.binancefuture.com/stream?streams=btcusdt@aggTrade/btcusdt@depth20@100ms/btcusdt@markPrice@1s"
    
    def on_message(ws, message):
        try:
            msg = json.loads(message)
            payload = msg.get("data", msg)
            event = payload.get("e", "")
            
            if event == "aggTrade":
                data.add_trade(payload)
            elif event == "depthUpdate":
                data.update_depth(payload)
            elif event == "markPriceUpdate":
                data.mark_price = float(payload.get("p", 0))
        except:
            pass
    
    def on_open(ws):
        print("  ✅ WS 已连接")
    
    def on_close(ws, *args):
        print("  ⚠️ WS 断开，5秒后重连...")
        time.sleep(5)
    
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"  ❌ WS 错误: {e}")
            time.sleep(5)

def analysis_worker():
    """后台分析线程"""
    while True:
        try:
            data.analyze()
            # 推送数据到前端
            snapshot = data.get_snapshot()
            socketio.emit('update', snapshot)
        except Exception as e:
            pass
        time.sleep(3)

# ==================== Web 路由 ====================

@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/status')
def api_status():
    return jsonify(data.get_snapshot())

@app.route('/api/balance')
def api_balance():
    try:
        return jsonify({"ok": True})
    except:
        return jsonify({"ok": False})

@app.route('/api/trade/<direction>/<float:qty>')
def api_trade(direction, qty):
    try:
        side = "BUY" if direction == "long" else "SELL"
        result = place_order("BTCUSDT", side, "MARKET", qty)
        return jsonify({"ok": True, "result": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

# ==================== HTML 模板 ====================

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>📊 Order Flow Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.4/socket.io.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: #0a0a0f; color: #e0e0e0; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 13px; }
.grid { display: grid; grid-template-columns: 1fr 1fr 1fr; grid-template-rows: auto auto 1fr; gap: 8px; padding: 8px; height: 100vh; }
.card { background: #12121a; border: 1px solid #1e1e2e; border-radius: 8px; padding: 12px; overflow: hidden; }
.card-title { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
.price-big { font-size: 32px; font-weight: bold; color: #00d4aa; }
.price-up { color: #00d4aa; }
.price-down { color: #ff4757; }
.metric { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #1a1a2e; }
.metric-label { color: #666; }
.metric-value { font-weight: bold; }
.positive { color: #00d4aa; }
.negative { color: #ff4757; }
.neutral { color: #ffa502; }
.signal-item { padding: 6px 8px; margin: 4px 0; border-radius: 4px; font-size: 12px; }
.signal-bullish { background: rgba(0,212,170,0.1); border-left: 3px solid #00d4aa; }
.signal-bearish { background: rgba(255,71,87,0.1); border-left: 3px solid #ff4757; }
.signal-neutral { background: rgba(255,165,2,0.1); border-left: 3px solid #ffa502; }
.consensus-box { text-align: center; padding: 16px; border-radius: 8px; font-size: 18px; font-weight: bold; }
.consensus-bullish { background: rgba(0,212,170,0.15); color: #00d4aa; border: 1px solid #00d4aa; }
.consensus-bearish { background: rgba(255,71,87,0.15); color: #ff4757; border: 1px solid #ff4757; }
.consensus-neutral { background: rgba(255,165,2,0.15); color: #ffa502; border: 1px solid #ffa502; }
.chart-container { position: relative; height: 180px; }
.dom-row { display: flex; align-items: center; padding: 2px 0; font-size: 12px; }
.dom-price { width: 70px; text-align: right; padding-right: 8px; }
.dom-bar { flex: 1; height: 16px; border-radius: 2px; position: relative; }
.dom-bar-bid { background: rgba(0,212,170,0.3); }
.dom-bar-ask { background: rgba(255,71,87,0.3); }
.dom-qty { width: 80px; text-align: right; padding-left: 8px; font-size: 11px; }
.vp-row { display: flex; align-items: center; padding: 1px 0; }
.vp-price { width: 60px; text-align: right; padding-right: 6px; font-size: 11px; color: #888; }
.vp-bar { flex: 1; height: 12px; border-radius: 2px; }
.vp-bar-normal { background: rgba(100,100,200,0.3); }
.vp-bar-poc { background: rgba(255,165,2,0.5); }
.vp-bar-hvn { background: rgba(100,100,200,0.5); }
.vp-vol { width: 60px; text-align: right; font-size: 10px; color: #666; }
.status-bar { grid-column: 1 / -1; display: flex; gap: 16px; align-items: center; padding: 6px 12px; background: #0d0d14; border-radius: 6px; font-size: 12px; }
.status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.status-dot.online { background: #00d4aa; box-shadow: 0 0 6px #00d4aa; }
.status-dot.offline { background: #ff4757; }
</style>
</head>
<body>
<div class="grid">
  <!-- 顶栏 -->
  <div class="status-bar">
    <span><span class="status-dot online" id="wsStatus"></span> WebSocket</span>
    <span>📊 Trades: <span id="tradeCount">0</span></span>
    <span>⚡ <span id="msgRate">0</span>/s</span>
    <span>⏱️ <span id="latency">0</span>ms</span>
    <span id="clock"></span>
  </div>

  <!-- 左列: 价格 + 信号 -->
  <div style="display:flex;flex-direction:column;gap:8px;">
    <div class="card">
      <div class="card-title">💰 BTCUSDT</div>
      <div class="price-big" id="price">--</div>
      <div style="display:flex;gap:16px;margin-top:8px;">
        <div><span style="color:#666">Bid</span> <span id="bid" class="positive">--</span></div>
        <div><span style="color:#666">Ask</span> <span id="ask" class="negative">--</span></div>
        <div><span style="color:#666">Spread</span> <span id="spread">--</span></div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">📊 Delta / CVD</div>
      <div class="metric"><span class="metric-label">CVD</span><span class="metric-value" id="cvd">0</span></div>
      <div class="metric"><span class="metric-label">Delta</span><span class="metric-value" id="delta">0</span></div>
      <div class="metric"><span class="metric-label">POC</span><span class="metric-value" id="poc">--</span></div>
      <div class="metric"><span class="metric-label">VAH</span><span class="metric-value" id="vah">--</span></div>
      <div class="metric"><span class="metric-label">VAL</span><span class="metric-value" id="val">--</span></div>
    </div>

    <div class="card">
      <div class="card-title">🎯 共识信号</div>
      <div class="consensus-box consensus-neutral" id="consensusBox">
        ⚪ 观望 (0%)
      </div>
      <div id="signalList" style="margin-top:8px;max-height:120px;overflow-y:auto;"></div>
    </div>
  </div>

  <!-- 中列: 图表 -->
  <div style="display:flex;flex-direction:column;gap:8px;">
    <div class="card">
      <div class="card-title">📈 价格 & CVD</div>
      <div class="chart-container"><canvas id="priceChart"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title">📊 Delta 柱状图</div>
      <div class="chart-container"><canvas id="deltaChart"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title">⚡ 成交速度</div>
      <div class="chart-container"><canvas id="speedChart"></canvas></div>
    </div>
  </div>

  <!-- 右列: DOM + VP + 多空比 + 信号日志 -->
  <div style="display:flex;flex-direction:column;gap:8px;">
    <div class="card">
      <div class="card-title">📖 订单簿 (DOM)</div>
      <div id="domDisplay" style="max-height:200px;overflow-y:auto;"></div>
    </div>
    <div class="card">
      <div class="card-title">📊 成交量分布</div>
      <div id="vpDisplay" style="max-height:180px;overflow-y:auto;"></div>
    </div>
    <div class="card">
      <div class="card-title">⚖️ 多空比</div>
      <div id="lsDisplay">
        <div class="metric"><span class="metric-label">多头</span><span class="metric-value positive" id="longPct">--</span></div>
        <div class="metric"><span class="metric-label">空头</span><span class="metric-value negative" id="shortPct">--</span></div>
        <div class="metric"><span class="metric-label">比值</span><span class="metric-value" id="lsRatio">--</span></div>
      </div>
    </div>
    <div class="card">
      <div class="card-title">📋 信号日志</div>
      <div id="signalLog" style="max-height:150px;overflow-y:auto;font-size:11px;"></div>
    </div>
  </div>
</div>

<script>
const socket = io();

// Chart.js 配置
const chartOpts = (color) => ({
  responsive: true, maintainAspectRatio: false,
  animation: { duration: 0 },
  scales: { x: { display: false }, y: { grid: { color: '#1a1a2e' }, ticks: { color: '#666', font: { size: 10 } } } },
  plugins: { legend: { display: false } },
  elements: { point: { radius: 0 }, line: { borderWidth: 1.5 } },
});

let priceChart = new Chart(document.getElementById('priceChart'), {
  type: 'line',
  data: { labels: [], datasets: [
    { label: 'Price', data: [], borderColor: '#00d4aa', fill: false, yAxisID: 'y' },
    { label: 'CVD', data: [], borderColor: '#ffa502', fill: false, yAxisID: 'y1', borderDash: [5,3] },
  ]},
  options: {
    ...chartOpts(),
    scales: {
      x: { display: false },
      y: { position: 'left', grid: { color: '#1a1a2e' }, ticks: { color: '#666', font: { size: 10 } } },
      y1: { position: 'right', grid: { display: false }, ticks: { color: '#666', font: { size: 10 } } },
    }
  }
});

let deltaChart = new Chart(document.getElementById('deltaChart'), {
  type: 'bar',
  data: { labels: [], datasets: [{ data: [], backgroundColor: [] }] },
  options: chartOpts()
});

let speedChart = new Chart(document.getElementById('speedChart'), {
  type: 'line',
  data: { labels: [], datasets: [{ label: 'Speed', data: [], borderColor: '#5352ed', fill: true, backgroundColor: 'rgba(83,82,237,0.1)' }] },
  options: chartOpts()
});

function fmt(n, d=1) { return n != null ? Number(n).toLocaleString(undefined, {minimumFractionDigits:d, maximumFractionDigits:d}) : '--'; }

socket.on('update', (d) => {
  // 价格
  const priceEl = document.getElementById('price');
  priceEl.textContent = '$' + fmt(d.price, 1);
  priceEl.className = 'price-big ' + (d.consensus === 'bullish' ? 'price-up' : d.consensus === 'bearish' ? 'price-down' : '');
  document.getElementById('bid').textContent = fmt(d.bid, 1);
  document.getElementById('ask').textContent = fmt(d.ask, 1);
  document.getElementById('spread').textContent = fmt(d.spread, 1);
  
  // 指标
  const cvdEl = document.getElementById('cvd');
  cvdEl.textContent = (d.cvd >= 0 ? '+' : '') + fmt(d.cvd, 0);
  cvdEl.className = 'metric-value ' + (d.cvd >= 0 ? 'positive' : 'negative');
  document.getElementById('delta').textContent = (d.delta >= 0 ? '+' : '') + fmt(d.delta, 2);
  document.getElementById('poc').textContent = fmt(d.poc, 0);
  document.getElementById('vah').textContent = fmt(d.vah, 0);
  document.getElementById('val').textContent = fmt(d.val, 0);
  
  // 状态栏
  document.getElementById('tradeCount').textContent = d.trades;
  document.getElementById('msgRate').textContent = d.msg_per_sec;
  document.getElementById('latency').textContent = d.latency;
  
  // 共识
  const box = document.getElementById('consensusBox');
  const icons = {bullish:'🟢 做多', bearish:'🔴 做空', neutral:'⚪ 观望'};
  box.textContent = (icons[d.consensus] || '⚪ 观望') + ' (' + d.confidence + '%)';
  box.className = 'consensus-box consensus-' + d.consensus;
  
  // 信号列表
  let sigHtml = '';
  (d.signals || []).forEach(s => {
    const cls = s.bias === 'bullish' ? 'signal-bullish' : s.bias === 'bearish' ? 'signal-bearish' : 'signal-neutral';
    const icon = s.bias === 'bullish' ? '🟢' : s.bias === 'bearish' ? '🔴' : '⚪';
    sigHtml += '<div class="signal-item ' + cls + '">' + icon + ' ' + s.source + '</div>';
  });
  document.getElementById('signalList').innerHTML = sigHtml;
  
  // 多空比
  if (d.ls_data) {
    document.getElementById('longPct').textContent = d.ls_data.long_pct.toFixed(1) + '%';
    document.getElementById('shortPct').textContent = d.ls_data.short_pct.toFixed(1) + '%';
    document.getElementById('lsRatio').textContent = d.ls_data.ratio.toFixed(3);
  }
  
  // 信号日志
  let logHtml = '';
  (d.signal_log || []).reverse().forEach(s => {
    const icon = s.bias === 'bullish' ? '🟢' : s.bias === 'bearish' ? '🔴' : '⚪';
    logHtml += '<div style="padding:2px 0;border-bottom:1px solid #1a1a2e">' + s.time + ' ' + icon + ' ' + s.source + '</div>';
  });
  document.getElementById('signalLog').innerHTML = logHtml;
  
  // 图表更新
  if (d.price_history && d.price_history.length > 0) {
    const labels = d.price_history.map((p,i) => i);
    priceChart.data.labels = labels;
    priceChart.data.datasets[0].data = d.price_history.map(p => p.v);
    priceChart.data.datasets[1].data = d.cvd_history.map(p => p.v);
    priceChart.update();
  }
  
  if (d.delta_history && d.delta_history.length > 0) {
    const labels = d.delta_history.map((p,i) => i);
    deltaChart.data.labels = labels;
    deltaChart.data.datasets[0].data = d.delta_history.map(p => p.v);
    deltaChart.data.datasets[0].backgroundColor = d.delta_history.map(p => p.v >= 0 ? 'rgba(0,212,170,0.5)' : 'rgba(255,71,87,0.5)');
    deltaChart.update();
  }
  
  if (d.speed_history && d.speed_history.length > 0) {
    const labels = d.speed_history.map((p,i) => i);
    speedChart.data.labels = labels;
    speedChart.data.datasets[0].data = d.speed_history.map(p => p.v);
    speedChart.update();
  }
  
  // 订单簿
  let domHtml = '';
  const asks = (d.signals ? [] : []).concat([]);
  // 简化: 从 snapshot 获取
  if (d.vah) {
    domHtml = '<div style="text-align:center;color:#666;padding:8px;">实时 DOM 需要 depth 数据流</div>';
  }
  document.getElementById('domDisplay').innerHTML = domHtml;
  
  // Volume Profile
  if (d.volume_profile) {
    const entries = Object.entries(d.volume_profile).sort((a,b) => b[1]-a[1]).slice(0, 12);
    const maxVol = entries.length > 0 ? entries[0][1] : 1;
    let vpHtml = '';
    entries.sort((a,b) => parseFloat(b[0]) - parseFloat(a[0])).forEach(([price, vol]) => {
      const pct = (vol / maxVol * 100).toFixed(0);
      const isPoc = d.poc && Math.abs(parseFloat(price) - d.poc) < 1;
      const cls = isPoc ? 'vp-bar-poc' : 'vp-bar-normal';
      const marker = isPoc ? ' ◀ POC' : '';
      vpHtml += '<div class="vp-row"><span class="vp-price">' + parseFloat(price).toFixed(0) + '</span>';
      vpHtml += '<div class="vp-bar ' + cls + '" style="width:' + pct + '%"></div>';
      vpHtml += '<span class="vp-vol">' + fmt(vol, 1) + marker + '</span></div>';
    });
    document.getElementById('vpDisplay').innerHTML = vpHtml;
  }
});

// 时钟
setInterval(() => {
  document.getElementById('clock').textContent = new Date().toISOString().substring(11,19) + ' UTC';
}, 1000);
</script>
</body>
</html>
"""

# ==================== 启动 ====================

if __name__ == '__main__':
    print("="*50)
    print("  📊 Order Flow Dashboard")
    print("  http://0.0.0.0:8765")
    print("="*50)
    
    # 启动后台线程
    threading.Thread(target=ws_worker, daemon=True).start()
    threading.Thread(target=analysis_worker, daemon=True).start()
    
    # 启动 Flask
    socketio.run(app, host='0.0.0.0', port=8765, debug=False, allow_unsafe_werkzeug=True)
