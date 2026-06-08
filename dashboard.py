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
from datetime import datetime, timezone, timedelta

BJT = timezone(timedelta(hours=8))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# SOCKS5 代理 — 从 config 读取
def _setup_proxy():
    try:
        from config import PROXY_ENABLED, SOCKS5_PROXY
        if PROXY_ENABLED and SOCKS5_PROXY:
            import socks
            import socket as sock_module
            proxy_str = SOCKS5_PROXY.replace("socks5://", "").replace("socks5h://", "")
            host, port = proxy_str.split(":")
            socks.set_default_proxy(socks.SOCKS5, host, int(port))
            sock_module.socket = socks.socksocket
            return True
    except ImportError:
        pass
    return False

_setup_proxy()
import websocket
from flask import Flask, render_template_string, jsonify
from flask_socketio import SocketIO
from orderflow import (
    FootprintChart, DeltaTracker, VolumeProfile,
    ImbalanceDetector, AbsorptionDetector, ExhaustionDetector,
    IcebergDetector, SpeedOfTape, OrderFlowSignalEngine, MarketReasoning
)
from trader import get_balance, get_positions, place_order, get_base_url, get_current_mode, get_all_orders, get_my_trades, api_request

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
        self.reasoner = MarketReasoning()
        self.signals = []
        self.consensus = "neutral"
        self.confidence = 0
        
        # 历史数据（用于图表）
        self.cvd_history = deque(maxlen=200)
        self.price_history = deque(maxlen=200)
        self.delta_history = deque(maxlen=200)
        self.speed_history = deque(maxlen=200)
        self.signal_log = deque(maxlen=50)
        
        # 分析报告（每 2 分钟更新）
        self.analysis_report = None
        self.analysis_time = 0
        self.analysis_history = deque(maxlen=50)  # 保存历史推理
        
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
                "time": datetime.now(BJT).strftime("%H:%M:%S"),
                "source": s.get("source", "?"),
                "type": s.get("type", "?"),
                "bias": s.get("bias", "neutral"),
                "detail": s.get("reason", ""),
            }
            if not self.signal_log or self.signal_log[-1] != entry:
                self.signal_log.append(entry)
    
    def generate_analysis(self):
        """生成走势分析报告（每 2 分钟更新一次）"""
        now = time.time()
        if self.analysis_report and (now - self.analysis_time) < 120:
            return self.analysis_report

        trades = list(self.trades)[-2000:]
        if len(trades) < 100:
            return None

        try:
            price = float(trades[-1]["p"])
            cvd = self.engine.delta.cvd
            delta = self.engine.delta.current_delta
            vah, val, poc = self.engine.volume_profile.get_value_area()
            speed = self.engine.speed
            momentum = speed.get_momentum() if hasattr(speed, 'get_momentum') else 'unknown'

            # 计算 ATR
            prices = [float(t["p"]) for t in trades[-200:]]
            atr_pct = 0.0
            if len(prices) > 14:
                trs = []
                for i in range(1, len(prices)):
                    trs.append(abs(prices[i] - prices[i-1]))
                atr = sum(trs[-14:]) / 14
                atr_pct = atr / price * 100 if price else 0

            # 用 MarketReasoning 生成推理报告（AI 优先，规则兜底）
            report = self.reasoner.analyze_with_ai(
                price=price,
                poc=poc, vah=vah, val=val,
                cvd=cvd, delta=delta,
                signals=self.signals,
                trades=trades,
                depth_bids=self.depth.get("bids"),
                depth_asks=self.depth.get("asks"),
                atr_pct=atr_pct,
            )

            self.analysis_report = report
            self.analysis_time = now

            # 存入历史（带时间戳）
            history_entry = {
                "time": now,
                "time_str": datetime.now(BJT).strftime("%H:%M:%S"),
                "report": report,
            }
            self.analysis_history.append(history_entry)

            return report

        except Exception as e:
            return {"error": str(e)}
    
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
        
        # Footprint 数据（最近 6 根 5 分钟 K 线）
        footprint_data = []
        fp = self.engine.footprint
        if fp.bars:
            sorted_bars = sorted(fp.bars.items(), key=lambda x: x[0], reverse=True)[:6]
            for bar_time, price_levels in sorted_bars:
                bar_entry = {"time": bar_time, "levels": {}}
                for price, vol in price_levels.items():
                    bar_entry["levels"][str(price)] = {"buy": round(vol.get("buy", 0), 4), "sell": round(vol.get("sell", 0), 4)}
                footprint_data.append(bar_entry)
            footprint_data.reverse()  # 按时间正序

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
            "footprint": footprint_data,
            "depth": self.depth,
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

@app.route('/api/analysis')
def api_analysis():
    report = data.generate_analysis()
    if report:
        return jsonify(report)
    return jsonify({"error": "数据不足，等待更多成交"}), 503

@app.route('/api/analysis/history')
def api_analysis_history():
    history = list(data.analysis_history)
    return jsonify({"ok": True, "history": history})

@app.route('/api/trade/<direction>/<float:qty>')
def api_trade(direction, qty):
    try:
        side = "BUY" if direction == "long" else "SELL"
        result = place_order("BTCUSDT", side, "MARKET", qty)
        return jsonify({"ok": True, "result": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route('/api/orders')
def api_orders():
    """历史订单"""
    try:
        from trader import get_base_url, get_current_mode, api_request
        import time as _time
        mode = get_current_mode()
        base = get_base_url(mode)
        symbol = "BTCUSDT"
        url = f"{base}/fapi/v1/allOrders" if mode.startswith("futures") else f"{base}/api/v3/allOrders"
        params = {"symbol": symbol, "limit": 50}
        data = api_request("GET", url, params, signed=True, mode=mode)
        orders = []
        for o in (data or []):
            ts = o.get("time", 0)
            orders.append({
                "orderId": o.get("orderId"),
                "side": o.get("side"),
                "type": o.get("type"),
                "origQty": o.get("origQty"),
                "price": o.get("price"),
                "avgPrice": o.get("avgPrice", "0"),
                "status": o.get("status"),
                "time": ts,
                "time_str": _time.strftime("%m-%d %H:%M", _time.localtime(ts / 1000)) if ts else "",
            })
        return jsonify({"ok": True, "orders": orders})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route('/api/trades')
def api_trades():
    """成交记录"""
    try:
        from trader import get_base_url, get_current_mode, api_request
        import time as _time
        mode = get_current_mode()
        base = get_base_url(mode)
        symbol = "BTCUSDT"
        url = f"{base}/fapi/v1/userTrades" if mode.startswith("futures") else f"{base}/api/v3/myTrades"
        params = {"symbol": symbol, "limit": 50}
        data = api_request("GET", url, params, signed=True, mode=mode)
        trades = []
        total_pnl = 0.0
        for t in (data or []):
            pnl = float(t.get("realizedPnl", 0))
            ts = t.get("time", 0)
            trades.append({
                "side": t.get("side"),
                "qty": t.get("qty"),
                "price": t.get("price"),
                "quoteQty": t.get("quoteQty"),
                "commission": t.get("commission"),
                "realizedPnl": pnl,
                "maker": t.get("maker", False),
                "time": ts,
                "time_str": _time.strftime("%m-%d %H:%M:%S", _time.localtime(ts / 1000)) if ts else "",
            })
            total_pnl += pnl
        return jsonify({"ok": True, "trades": trades, "total_pnl": round(total_pnl, 4)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route('/api/positions')
def api_positions():
    """当前持仓"""
    try:
        mode = get_current_mode()
        base = get_base_url(mode)
        if not mode.startswith("futures"):
            return jsonify({"ok": True, "positions": []})
        url = f"{base}/fapi/v2/positionRisk"
        data = api_request("GET", url, signed=True, mode=mode)
        positions = []
        for p in (data or []):
            amt = float(p.get("positionAmt", 0))
            if amt == 0:
                continue
            positions.append({
                "symbol": p.get("symbol"),
                "side": "多" if amt > 0 else "空",
                "amt": abs(amt),
                "entryPrice": p.get("entryPrice"),
                "markPrice": p.get("markPrice"),
                "unRealizedProfit": p.get("unRealizedProfit"),
                "leverage": p.get("leverage"),
                "liquidationPrice": p.get("liquidationPrice"),
                "marginType": p.get("marginType"),
            })
        return jsonify({"ok": True, "positions": positions})
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
.grid { display: grid; grid-template-columns: 1fr 1fr 1fr; grid-template-rows: auto auto auto 1fr; gap: 8px; padding: 8px; min-height: 100vh; }
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

  <!-- 中列: 足迹图 + 图表 -->
  <div style="display:flex;flex-direction:column;gap:8px;">
    <div class="card" style="min-height:300px;">
      <div class="card-title">🔥 Footprint 热力图（ATAS 风格）</div>
      <canvas id="footprintCanvas" style="width:100%;height:260px;"></canvas>
    </div>
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

  <!-- 底部: 持仓 + 订单 + 成交（全宽）-->
  <div class="card" style="grid-column: 1 / -1;">
    <div class="card-title">📦 持仓 / 订单 / 成交 <button onclick="refreshOrders()" style="float:right;background:#1e1e2e;color:#aaa;border:1px solid #333;border-radius:4px;padding:2px 8px;cursor:pointer;font-size:11px;">刷新</button></div>
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;">
      <!-- 持仓 -->
      <div>
        <div style="font-size:12px;font-weight:bold;color:#888;margin-bottom:6px;">📊 当前持仓</div>
        <div id="positionsList" style="max-height:180px;overflow-y:auto;font-size:11px;">加载中...</div>
      </div>
      <!-- 历史订单 -->
      <div>
        <div style="font-size:12px;font-weight:bold;color:#888;margin-bottom:6px;">📋 历史订单</div>
        <div id="ordersList" style="max-height:180px;overflow-y:auto;font-size:11px;">加载中...</div>
      </div>
      <!-- 成交记录 -->
      <div>
        <div style="font-size:12px;font-weight:bold;color:#888;margin-bottom:6px;">💰 成交记录 <span id="totalPnl" style="float:right;"></span></div>
        <div id="tradesList" style="max-height:180px;overflow-y:auto;font-size:11px;">加载中...</div>
      </div>
    </div>
  </div>

  <!-- 底部: 交易员分析对话框（全宽，每 2 分钟更新）-->
  <div class="card" style="grid-column: 1 / -1;">
    <div class="card-title">🧠 交易员分析 <span style="float:right;color:#555;font-size:11px;">每 2 分钟自动更新 | 点击展开详情</span></div>
    <!-- 当前结论（始终可见） -->
    <div style="display:flex;gap:12px;margin-bottom:12px;">
      <div id="verdictBox" style="flex:0 0 200px;text-align:center;padding:16px;border-radius:8px;background:rgba(255,165,2,0.1);border:1px solid #ffa502;">
        <div style="font-size:11px;color:#888;">当前结论</div>
        <div id="verdictText" style="font-size:28px;font-weight:bold;margin:6px 0;">--</div>
        <div id="verdictConf" style="font-size:14px;color:#aaa;">置信度 --%</div>
      </div>
      <div style="flex:1;display:flex;flex-direction:column;gap:8px;">
        <div id="actionPlan" style="font-size:12px;">
          <div id="actionEntry" class="metric"><span class="metric-label">入场</span><span class="metric-value" id="aEntry">--</span></div>
          <div id="actionSL" class="metric"><span class="metric-label">止损</span><span class="metric-value negative" id="aSL">--</span></div>
          <div id="actionTP" class="metric"><span class="metric-label">止盈</span><span class="metric-value positive" id="aTP">--</span></div>
          <div id="actionRR" class="metric"><span class="metric-label">盈亏比</span><span class="metric-value" id="aRR">--</span></div>
          <div id="actionAdvice" style="margin-top:4px;padding:4px 6px;background:#0d0d14;border-radius:4px;font-size:11px;color:#888;">--</div>
        </div>
        <div id="riskNote" style="padding:6px 8px;background:rgba(255,71,87,0.08);border-radius:4px;font-size:11px;color:#ff4757;display:none;">
          <span style="font-weight:bold;">⚠️ </span><span id="riskNoteText"></span>
        </div>
      </div>
      <div style="flex:0 0 200px;font-size:11px;">
        <div style="font-weight:bold;color:#888;margin-bottom:6px;">📍 关键价位</div>
        <div class="metric"><span class="metric-label">POC</span><span class="metric-value" id="aPoc">--</span></div>
        <div class="metric"><span class="metric-label">VAH</span><span class="metric-value" id="aVah">--</span></div>
        <div class="metric"><span class="metric-label">VAL</span><span class="metric-value" id="aVal">--</span></div>
      </div>
    </div>
    <!-- 对话框：历史推理记录 -->
    <div id="chatContainer" style="height:400px;overflow-y:auto;padding:8px;background:#08080e;border-radius:6px;border:1px solid #1a1a2e;">
      <div style="text-align:center;color:#555;padding:40px 0;">等待分析数据...</div>
    </div>
    <div style="text-align:center;margin-top:6px;">
      <button onclick="loadAnalysisHistory()" style="background:#1e1e2e;color:#888;border:1px solid #333;border-radius:4px;padding:4px 16px;cursor:pointer;font-size:11px;">📜 加载历史记录</button>
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
  data: { labels: [], datasets: [{ data: [], backgroundColor: [], borderWidth: 0, barPercentage: 0.8 }] },
  options: {
    responsive: true, maintainAspectRatio: false,
    animation: { duration: 0 },
    scales: {
      x: { display: false },
      y: { grid: { color: '#1a1a2e' }, ticks: { color: '#666', font: { size: 10 } } }
    },
    plugins: { legend: { display: false } },
  }
});

let speedChart = new Chart(document.getElementById('speedChart'), {
  type: 'line',
  data: { labels: [], datasets: [{ label: 'Speed', data: [], borderColor: '#5352ed', fill: true, backgroundColor: 'rgba(83,82,237,0.1)' }] },
  options: chartOpts()
});

function fmt(n, d=1) { return n != null ? Number(n).toLocaleString(undefined, {minimumFractionDigits:d, maximumFractionDigits:d}) : '--'; }

// Footprint 热力图渲染
function drawFootprint(footprintData) {
  const canvas = document.getElementById('footprintCanvas');
  if (!canvas || !footprintData || footprintData.length === 0) return;
  
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  
  ctx.fillStyle = '#0a0a0f';
  ctx.fillRect(0, 0, W, H);
  
  // 收集所有价格层级
  let allPrices = new Set();
  footprintData.forEach(bar => {
    Object.keys(bar.levels).forEach(p => allPrices.add(parseFloat(p)));
  });
  allPrices = Array.from(allPrices).sort((a,b) => a - b);
  if (allPrices.length === 0) return;
  
  // 计算全局最大成交量（用于颜色映射）
  let maxVol = 0;
  footprintData.forEach(bar => {
    Object.values(bar.levels).forEach(v => {
      maxVol = Math.max(maxVol, v.buy, v.sell);
    });
  });
  if (maxVol === 0) return;
  
  const nBars = footprintData.length;
  const padding = { top: 20, bottom: 20, left: 60, right: 10 };
  const barAreaW = W - padding.left - padding.right;
  const barAreaH = H - padding.top - padding.bottom;
  const colW = barAreaW / nBars;
  const cellH = Math.max(8, barAreaH / allPrices.length);
  const halfCell = colW / 2 - 2;
  
  // 价格标签
  ctx.fillStyle = '#666';
  ctx.font = '10px monospace';
  ctx.textAlign = 'right';
  const labelStep = Math.max(1, Math.floor(allPrices.length / 15));
  for (let i = 0; i < allPrices.length; i += labelStep) {
    const y = padding.top + i * cellH + cellH / 2 + 3;
    ctx.fillText(allPrices[i].toFixed(0), padding.left - 6, y);
  }
  
  // 绘制每根 K 线的 footprint
  footprintData.forEach((bar, barIdx) => {
    const x0 = padding.left + barIdx * colW;
    
    // 时间标签
    ctx.fillStyle = '#555';
    ctx.font = '9px monospace';
    ctx.textAlign = 'center';
    const t = new Date(bar.time * 1000);
    ctx.fillText(t.toTimeString().slice(0,5), x0 + colW/2, H - 4);
    
    allPrices.forEach((price, priceIdx) => {
      const key = price.toFixed(1);
      const level = bar.levels[key];
      if (!level) return;
      
      const y = padding.top + priceIdx * cellH;
      const buyVol = level.buy;
      const sellVol = level.sell;
      
      if (buyVol > 0) {
        const intensity = Math.min(1, buyVol / maxVol);
        const width = Math.max(2, intensity * halfCell);
        ctx.fillStyle = `rgba(0, 212, 170, ${0.2 + intensity * 0.6})`;
        ctx.fillRect(x0 + colW/2 - width, y + 1, width, cellH - 2);
        // 数量文字
        if (intensity > 0.15) {
          ctx.fillStyle = `rgba(0, 212, 170, ${0.6 + intensity * 0.4})`;
          ctx.font = '9px monospace';
          ctx.textAlign = 'right';
          ctx.fillText(buyVol.toFixed(1), x0 + colW/2 - 2, y + cellH/2 + 3);
        }
      }
      
      if (sellVol > 0) {
        const intensity = Math.min(1, sellVol / maxVol);
        const width = Math.max(2, intensity * halfCell);
        ctx.fillStyle = `rgba(255, 71, 87, ${0.2 + intensity * 0.6})`;
        ctx.fillRect(x0 + colW/2, y + 1, width, cellH - 2);
        // 数量文字
        if (intensity > 0.15) {
          ctx.fillStyle = `rgba(255, 71, 87, ${0.6 + intensity * 0.4})`;
          ctx.font = '9px monospace';
          ctx.textAlign = 'left';
          ctx.fillText(sellVol.toFixed(1), x0 + colW/2 + 2, y + cellH/2 + 3);
        }
      }
    });
  });
  
  // 图例
  ctx.font = '10px monospace';
  ctx.textAlign = 'left';
  ctx.fillStyle = '#00d4aa';
  ctx.fillRect(W - 130, 4, 10, 10);
  ctx.fillText('Taker Buy', W - 116, 13);
  ctx.fillStyle = '#ff4757';
  ctx.fillRect(W - 60, 4, 10, 10);
  ctx.fillText('Sell', W - 46, 13);
}

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
  
  // 订单簿 DOM
  if (d.depth && d.depth.bids && d.depth.asks) {
    const bids = d.depth.bids.slice(0, 8);
    const asks = d.depth.asks.slice(0, 8).reverse();
    const maxQty = Math.max(...bids.map(b=>b[1]), ...asks.map(a=>a[1]), 1);
    let domHtml = '';
    // 卖方（红）
    asks.forEach(([price, qty]) => {
      const pct = (qty / maxQty * 100).toFixed(0);
      domHtml += '<div class="dom-row"><span class="dom-price" style="color:#ff4757">' + fmt(price,1) + '</span>';
      domHtml += '<div class="dom-bar dom-bar-ask" style="width:' + pct + '%"></div>';
      domHtml += '<span class="dom-qty" style="color:#ff4757">' + fmt(qty,2) + '</span></div>';
    });
    domHtml += '<div style="text-align:center;padding:4px;color:#888;border-top:1px solid #1e1e2e;border-bottom:1px solid #1e1e2e;margin:2px 0;">' + fmt(d.price,1) + '</div>';
    // 买方（绿）
    bids.forEach(([price, qty]) => {
      const pct = (qty / maxQty * 100).toFixed(0);
      domHtml += '<div class="dom-row"><span class="dom-price" style="color:#00d4aa">' + fmt(price,1) + '</span>';
      domHtml += '<div class="dom-bar dom-bar-bid" style="width:' + pct + '%"></div>';
      domHtml += '<span class="dom-qty" style="color:#00d4aa">' + fmt(qty,2) + '</span></div>';
    });
    document.getElementById('domDisplay').innerHTML = domHtml;
  }
  
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
  
  // Footprint 热力图
  if (d.footprint) drawFootprint(d.footprint);
});

// 时钟
setInterval(() => {
  const now = new Date();
  const bj = new Date(now.getTime() + 8 * 3600000);
  document.getElementById('clock').textContent = bj.toISOString().substring(11,19) + ' 北京时间';
}, 1000);

// === 持仓 / 订单 / 成交 ===
function refreshOrders() {
  // 持仓
  fetch('/api/positions').then(r=>r.json()).then(d => {
    const el = document.getElementById('positionsList');
    if (!d.ok || !d.positions || d.positions.length === 0) {
      el.innerHTML = '<div style="color:#666">📭 无持仓</div>';
      return;
    }
    let html = '';
    d.positions.forEach(p => {
      const isLong = p.side === '多';
      const color = isLong ? '#00d4aa' : '#ff4757';
      const pnl = parseFloat(p.unRealizedProfit || 0);
      const pnlColor = pnl >= 0 ? '#00d4aa' : '#ff4757';
      html += '<div style="padding:4px 0;border-bottom:1px solid #1a1a2e">';
      html += '<span style="color:' + color + ';font-weight:bold">' + p.side + '</span> ';
      html += '<span>' + p.amt + ' ' + p.symbol + '</span> ';
      html += '<span style="color:#888">@ ' + fmt(parseFloat(p.entryPrice),1) + '</span><br>';
      html += '<span style="color:#666">标记: ' + fmt(parseFloat(p.markPrice),1) + '</span> ';
      html += '<span style="color:' + pnlColor + '">浮盈: ' + pnl.toFixed(4) + ' USDT</span> ';
      html += '<span style="color:#666">杠杆: ' + p.leverage + 'x</span>';
      html += '</div>';
    });
    el.innerHTML = html;
  }).catch(() => { document.getElementById('positionsList').innerHTML = '<div style="color:#666">加载失败</div>'; });

  // 历史订单
  fetch('/api/orders').then(r=>r.json()).then(d => {
    const el = document.getElementById('ordersList');
    if (!d.ok || !d.orders || d.orders.length === 0) {
      el.innerHTML = '<div style="color:#666">📭 无历史订单</div>';
      return;
    }
    let html = '';
    d.orders.slice(0, 15).forEach(o => {
      const statusIcons = {FILLED:'✅', CANCELED:'❌', EXPIRED:'⏰', NEW:'🔵', PARTIALLY_FILLED:'🟡'};
      const icon = statusIcons[o.status] || '❓';
      const sideColor = o.side === 'BUY' ? '#00d4aa' : '#ff4757';
      const avgP = parseFloat(o.avgPrice || 0);
      const avgStr = avgP > 0 ? ' avg=' + fmt(avgP,1) : '';
      html += '<div style="padding:3px 0;border-bottom:1px solid #1a1a2e">';
      html += icon + ' <span style="color:' + sideColor + '">' + o.side + '</span> ';
      html += o.origQty + ' @ ' + o.price + avgStr;
      html += ' <span style="color:#666">' + o.time_str + '</span>';
      html += ' <span style="color:#555">[' + o.status + ']</span>';
      html += '</div>';
    });
    el.innerHTML = html;
  }).catch(() => { document.getElementById('ordersList').innerHTML = '<div style="color:#666">加载失败</div>'; });

  // 成交记录
  fetch('/api/trades').then(r=>r.json()).then(d => {
    const el = document.getElementById('tradesList');
    const pnlEl = document.getElementById('totalPnl');
    if (!d.ok || !d.trades || d.trades.length === 0) {
      el.innerHTML = '<div style="color:#666">📭 无成交记录</div>';
      return;
    }
    if (pnlEl && d.total_pnl !== undefined) {
      const c = d.total_pnl >= 0 ? '#00d4aa' : '#ff4757';
      pnlEl.innerHTML = '<span style="color:' + c + '">总PnL: ' + d.total_pnl.toFixed(4) + ' USDT</span>';
    }
    let html = '';
    d.trades.slice(0, 15).forEach(t => {
      const sideColor = t.side === 'BUY' ? '#00d4aa' : '#ff4757';
      const pnl = parseFloat(t.realizedPnl || 0);
      const pnlStr = pnl !== 0 ? ' <span style="color:' + (pnl>=0?'#00d4aa':'#ff4757') + '">PnL=' + pnl.toFixed(4) + '</span>' : '';
      const makerStr = t.maker ? '🟡M' : '🔵T';
      html += '<div style="padding:3px 0;border-bottom:1px solid #1a1a2e">';
      html += '<span style="color:' + sideColor + '">' + t.side + '</span> ';
      html += t.qty + ' @ ' + fmt(parseFloat(t.price),1);
      html += ' <span style="color:#666">' + t.time_str + '</span>';
      html += pnlStr + ' ' + makerStr;
      html += '</div>';
    });
    el.innerHTML = html;
  }).catch(() => { document.getElementById('tradesList').innerHTML = '<div style="color:#666">加载失败</div>'; });
}

// 首次加载 + 每 60 秒刷新
refreshOrders();
setInterval(refreshOrders, 60000);

// === 走势分析（对话框样式，每 2 分钟刷新）===
function fmtPrice(n) { return n ? '$' + Number(n).toLocaleString(undefined, {maximumFractionDigits:1}) : '--'; }

// 存储已渲染的消息，避免重复
window._renderedTimes = new Set();

function buildChatBubble(d, timeStr, isHistory) {
  const verdictColors = {LONG:'#00d4aa', SHORT:'#ff4757', WAIT:'#ffa502', AVOID:'#ff4757'};
  const verdictIcons = {LONG:'🟢', SHORT:'🔴', WAIT:'⚪', AVOID:'🚫'};
  const vColor = verdictColors[d.verdict] || '#ffa502';
  const vIcon = verdictIcons[d.verdict] || '⚪';
  const verdictZh = d.verdict_zh || '--';
  const conf = d.confidence || 0;

  // 行动方案
  const ap = d.action_plan || {};
  let actionHtml = '';
  if (d.verdict === 'LONG' || d.verdict === 'SHORT') {
    actionHtml = '<div style="margin-top:8px;padding:6px 8px;background:rgba(255,255,255,0.03);border-radius:4px;font-size:11px;">'
      + '<span style="color:#888;">入场</span> <b>' + fmtPrice(ap.entry) + '</b> | '
      + '<span style="color:#ff4757;">止损</span> <b>' + fmtPrice(ap.stop_loss) + '</b> | '
      + '<span style="color:#00d4aa;">止盈</span> <b>' + fmtPrice(ap.take_profit) + '</b> | '
      + '<span style="color:#888;">盈亏比</span> <b>' + (ap.risk_reward || 0) + 'R</b>'
      + (ap.position_advice ? '<br><span style="color:#666;">' + ap.position_advice + '</span>' : '')
      + '</div>';
  } else {
    actionHtml = '<div style="margin-top:8px;padding:6px 8px;background:rgba(255,255,255,0.03);border-radius:4px;font-size:11px;color:#888;">'
      + (ap.reason || '等待更好的机会')
      + (ap.watch_for && ap.watch_for.length > 0 ? '<br>👀 ' + ap.watch_for.join(' | ') : '')
      + '</div>';
  }

  // 推理步骤
  const steps = d.reasoning_steps || [];
  let stepsHtml = '';
  if (steps.length > 0) {
    stepsHtml = '<div class="chat-steps" style="display:none;margin-top:8px;padding:8px;background:#0a0a12;border-radius:4px;font-size:11px;line-height:1.7;">';
    steps.forEach(s => {
      if (s && s.thought) {
        stepsHtml += '<div style="padding:3px 0;border-bottom:1px solid #151520;">' + s.thought + '</div>';
      }
    });
    stepsHtml += '</div>';
  }

  // 多空因素
  const bulls = d.factors_bull || [];
  const bears = d.factors_bear || [];
  let factorsHtml = '<div class="chat-factors" style="display:none;margin-top:6px;font-size:11px;">';
  if (bulls.length > 0) {
    factorsHtml += '<div style="color:#00d4aa;font-weight:bold;">多方</div>';
    bulls.forEach(f => { factorsHtml += '<div style="padding:1px 0;">🟢 ' + f[0] + ' <span style="color:#555">(w' + f[1] + ')</span></div>'; });
  }
  if (bears.length > 0) {
    factorsHtml += '<div style="color:#ff4757;font-weight:bold;margin-top:4px;">空方</div>';
    bears.forEach(f => { factorsHtml += '<div style="padding:1px 0;">🔴 ' + f[0] + ' <span style="color:#555">(w' + f[1] + ')</span></div>'; });
  }
  factorsHtml += '</div>';

  // 关键价位
  const lv = d.levels || {};
  let levelsHtml = '';
  if (lv.poc || lv.vah || lv.val) {
    levelsHtml = '<div class="chat-levels" style="display:none;margin-top:6px;font-size:11px;color:#888;">'
      + 'POC=' + fmtPrice(lv.poc) + ' VAH=' + fmtPrice(lv.vah) + ' VAL=' + fmtPrice(lv.val) + '</div>';
  }

  // 风险提示
  const riskNote = d.risk_note || '';
  let riskHtml = '';
  if (riskNote && riskNote.length > 10) {
    riskHtml = '<div style="margin-top:6px;padding:4px 6px;background:rgba(255,71,87,0.08);border-radius:4px;font-size:11px;color:#ff4757;">⚠️ ' + riskNote.replace(/^【风险】/, '') + '</div>';
  }

  // 组装气泡
  const bubble = document.createElement('div');
  bubble.className = 'chat-bubble';
  bubble.style.cssText = 'padding:10px 12px;margin:6px 0;border-radius:6px;background:#12121a;border-left:3px solid ' + vColor + ';cursor:pointer;';
  bubble.setAttribute('data-time', timeStr);

  bubble.innerHTML = ''
    + '<div style="display:flex;justify-content:space-between;align-items:center;">'
    + '  <div style="font-size:11px;color:#555;">' + timeStr + (isHistory ? '' : ' · 最新') + '</div>'
    + '  <div style="font-size:13px;font-weight:bold;color:' + vColor + ';">' + vIcon + ' ' + verdictZh + ' <span style="font-size:11px;color:#888;">' + conf + '%</span></div>'
    + '</div>'
    + '<div style="font-size:11px;color:#aaa;margin-top:4px;">' + (d.verdict_reason || '') + '</div>'
    + actionHtml
    + riskHtml
    + stepsHtml
    + factorsHtml
    + levelsHtml
    + '<div style="text-align:right;margin-top:4px;"><span style="font-size:10px;color:#444;">点击展开/收起详情</span></div>';

  // 点击展开/收起详情
  bubble.addEventListener('click', function() {
    const details = bubble.querySelectorAll('.chat-steps, .chat-factors, .chat-levels');
    details.forEach(el => {
      el.style.display = el.style.display === 'none' ? 'block' : 'none';
    });
  });

  return bubble;
}

function appendChatBubble(d, timeStr, isHistory) {
  const container = document.getElementById('chatContainer');
  if (!container) return;

  // 去掉"等待数据"占位
  const placeholder = container.querySelector('div[style*="text-align:center"]');
  if (placeholder && placeholder.textContent.includes('等待')) {
    container.innerHTML = '';
  }

  // 避免重复（同一时间戳只渲染一次）
  if (window._renderedTimes.has(timeStr)) return;
  window._renderedTimes.add(timeStr);

  const bubble = buildChatBubble(d, timeStr, isHistory);
  container.appendChild(bubble);

  // 自动滚动到底部
  container.scrollTop = container.scrollHeight;
}

function updateAnalysis(d) {
  if (!d || d.error) return;

  const timeStr = d.time_str || new Date().toTimeString().slice(0,8);

  // 更新顶部当前结论
  const verdictColors = {LONG:'#00d4aa', SHORT:'#ff4757', WAIT:'#ffa502', AVOID:'#ff4757'};
  const vColor = verdictColors[d.verdict] || '#ffa502';
  const verdictBox = document.getElementById('verdictBox');
  if (d.verdict === 'LONG') verdictBox.style.background = 'rgba(0,212,170,0.15)';
  else if (d.verdict === 'SHORT') verdictBox.style.background = 'rgba(255,71,87,0.15)';
  else verdictBox.style.background = 'rgba(255,165,2,0.15)';
  verdictBox.style.borderColor = vColor;
  document.getElementById('verdictText').textContent = d.verdict_zh || '--';
  document.getElementById('verdictText').style.color = vColor;
  document.getElementById('verdictConf').textContent = '置信度 ' + (d.confidence || 0) + '%';

  // 行动方案
  const ap = d.action_plan || {};
  if (d.verdict === 'LONG' || d.verdict === 'SHORT') {
    document.getElementById('actionEntry').style.display = '';
    document.getElementById('actionSL').style.display = '';
    document.getElementById('actionTP').style.display = '';
    document.getElementById('actionRR').style.display = '';
    document.getElementById('aEntry').textContent = fmtPrice(ap.entry);
    document.getElementById('aSL').textContent = fmtPrice(ap.stop_loss);
    document.getElementById('aTP').textContent = fmtPrice(ap.take_profit);
    document.getElementById('aRR').textContent = (ap.risk_reward || 0) + 'R';
    document.getElementById('actionAdvice').textContent = ap.position_advice || '';
  } else {
    document.getElementById('actionEntry').style.display = 'none';
    document.getElementById('actionSL').style.display = 'none';
    document.getElementById('actionTP').style.display = 'none';
    document.getElementById('actionRR').style.display = 'none';
    document.getElementById('actionAdvice').textContent = ap.reason || '等待更好的机会';
  }

  // 关键价位
  const lv = d.levels || {};
  document.getElementById('aPoc').textContent = fmtPrice(lv.poc);
  document.getElementById('aVah').textContent = fmtPrice(lv.vah);
  document.getElementById('aVal').textContent = fmtPrice(lv.val);

  // 风险提示
  const riskNote = d.risk_note || '';
  const riskBox = document.getElementById('riskNote');
  if (riskNote && riskNote.length > 10) {
    riskBox.style.display = 'block';
    document.getElementById('riskNoteText').textContent = riskNote.replace(/^【风险】/, '');
  } else {
    riskBox.style.display = 'none';
  }

  // 追加到对话框
  appendChatBubble(d, timeStr, false);
}

// 首次加载 + 每 2 分钟刷新（失败时 10 秒重试）
function fetchAnalysis() {
  fetch('/api/analysis').then(r => {
    if (!r.ok) throw new Error('not ready');
    return r.json();
  }).then(d => {
    if (d && !d.error) {
      updateAnalysis(d);
      window._analysisOk = true;
    } else {
      throw new Error(d.error);
    }
  }).catch(() => {
    if (!window._analysisOk) setTimeout(fetchAnalysis, 10000);
  });
}
fetchAnalysis();
setInterval(fetchAnalysis, 120000);

// 加载历史推理记录
function loadAnalysisHistory() {
  fetch('/api/analysis/history').then(r => r.json()).then(d => {
    if (!d.ok || !d.history) return;
    const container = document.getElementById('chatContainer');
    container.innerHTML = '';
    window._renderedTimes = new Set();
    d.history.forEach(h => {
      if (h.report && !h.report.error) {
        appendChatBubble(h.report, h.time_str, true);
      }
    });
  }).catch(() => {});
}
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
