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
    IcebergDetector, SpeedOfTape, OrderFlowSignalEngine
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
            consensus, confidence = self.engine.get_consensus()
            speed = self.engine.speed
            momentum = speed.get_momentum() if hasattr(speed, 'get_momentum') else 'unknown'
            
            # === 趋势判断 ===
            # CVD 方向
            cvd_trend = "买方主导" if cvd > 500 else "卖方主导" if cvd < -500 else "多空均衡"
            
            # 价格 vs POC
            if poc:
                poc_dist = (price - poc) / poc * 100
                if poc_dist > 0.3:
                    price_pos = f"价格在 POC 上方 {poc_dist:.2f}%，偏强"
                elif poc_dist < -0.3:
                    price_pos = f"价格在 POC 下方 {abs(poc_dist):.2f}%，偏弱"
                else:
                    price_pos = "价格贴近 POC，震荡整理"
            else:
                poc_dist = 0
                price_pos = "数据不足"
            
            # === 信号汇总 ===
            bullish_count = sum(1 for s in self.signals if s.get("bias") == "bullish")
            bearish_count = sum(1 for s in self.signals if s.get("bias") == "bearish")
            
            signal_names = {
                "stacked_imbalance": "堆叠失衡",
                "absorption": "吸收",
                "exhaustion": "衰竭",
                "iceberg": "冰山单",
                "speed_of_tape": "成交速度",
            }
            
            active_signals = []
            for s in self.signals:
                name = signal_names.get(s.get("source", ""), s.get("source", "?"))
                bias = "看多" if s.get("bias") == "bullish" else "看空" if s.get("bias") == "bearish" else "中性"
                active_signals.append(f"{name}({bias})")
            
            # === 成交速度 ===
            if momentum == "accelerating_buy":
                speed_desc = "买方加速入场"
            elif momentum == "accelerating_sell":
                speed_desc = "卖方加速入场"
            elif momentum == "decelerating":
                speed_desc = "动能衰减"
            else:
                speed_desc = "速度平稳"
            
            # === 交易建议 ===
            if consensus == "bullish" and confidence > 60 and bullish_count >= 2:
                direction = "做多"
                direction_en = "LONG"
                entry = price
                sl = price * 0.9935
                tp = price * 1.013
                reason = f"多方信号 {bullish_count} 个一致，CVD {cvd_trend}，{speed_desc}"
            elif consensus == "bearish" and confidence > 60 and bearish_count >= 2:
                direction = "做空"
                direction_en = "SHORT"
                entry = price
                sl = price * 1.0065
                tp = price * 0.987
                reason = f"空方信号 {bearish_count} 个一致，CVD {cvd_trend}，{speed_desc}"
            else:
                direction = "观望"
                direction_en = "WAIT"
                entry = sl = tp = 0
                reason = f"信号不够一致（多{bullish_count}/空{bearish_count}），等待明确方向"
            
            # === 综合评分 ===
            score = 50  # 中性基准
            if cvd > 1000: score += 10
            elif cvd < -1000: score -= 10
            if poc_dist > 0.3: score += 8
            elif poc_dist < -0.3: score -= 8
            if bullish_count > bearish_count: score += min(bullish_count * 8, 20)
            elif bearish_count > bullish_count: score -= min(bearish_count * 8, 20)
            if "accelerating_buy" in str(momentum): score += 7
            elif "accelerating_sell" in str(momentum): score -= 7
            score = max(0, min(100, score))
            
            if score >= 70:
                outlook = "偏多"
            elif score >= 55:
                outlook = "略偏多"
            elif score <= 30:
                outlook = "偏空"
            elif score <= 45:
                outlook = "略偏空"
            else:
                outlook = "中性震荡"
            
            report = {
                "timestamp": now,
                "time_str": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                "price": price,
                "score": score,
                "outlook": outlook,
                "trend": {
                    "cvd": cvd,
                    "cvd_desc": cvd_trend,
                    "delta": delta,
                    "price_pos": price_pos,
                    "speed": speed_desc,
                    "momentum": momentum,
                },
                "levels": {
                    "poc": poc,
                    "vah": vah,
                    "val": val,
                    "poc_dist_pct": round(poc_dist, 3),
                },
                "signals": {
                    "active": active_signals,
                    "bullish": bullish_count,
                    "bearish": bearish_count,
                    "consensus": consensus,
                    "confidence": round(confidence * 100),
                },
                "recommendation": {
                    "direction": direction,
                    "direction_en": direction_en,
                    "entry": round(entry, 1),
                    "stop_loss": round(sl, 1),
                    "take_profit": round(tp, 1),
                    "reason": reason,
                },
            }
            
            self.analysis_report = report
            self.analysis_time = now
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

  <!-- 底部: 走势分析（全宽，每 2 分钟更新）-->
  <div class="card" style="grid-column: 1 / -1;">
    <div class="card-title">🧠 走势分析 <span id="analysisTime" style="float:right;color:#555;">--</span></div>
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;">
      <!-- 左: 综合评分 + 方向 -->
      <div>
        <div style="text-align:center;margin-bottom:12px;">
          <div style="font-size:14px;color:#888;margin-bottom:4px;">综合评分</div>
          <div id="analysisScore" style="font-size:48px;font-weight:bold;color:#ffa502;">--</div>
          <div id="analysisOutlook" style="font-size:16px;margin-top:4px;">--</div>
        </div>
        <div id="analysisRecommendation" style="text-align:center;padding:12px;border-radius:8px;background:rgba(255,165,2,0.1);border:1px solid #ffa502;">
          <div style="font-size:11px;color:#888;">交易建议</div>
          <div id="recDirection" style="font-size:22px;font-weight:bold;margin:4px 0;">--</div>
          <div id="recReason" style="font-size:11px;color:#aaa;">--</div>
        </div>
        <div id="recLevels" style="margin-top:8px;font-size:12px;">
          <div class="metric"><span class="metric-label">入场</span><span class="metric-value" id="recEntry">--</span></div>
          <div class="metric"><span class="metric-label">止损</span><span class="metric-value negative" id="recSL">--</span></div>
          <div class="metric"><span class="metric-label">止盈</span><span class="metric-value positive" id="recTP">--</span></div>
        </div>
      </div>
      <!-- 中: 趋势分析 -->
      <div>
        <div style="font-size:13px;font-weight:bold;color:#888;margin-bottom:8px;">📊 趋势判断</div>
        <div class="metric"><span class="metric-label">CVD 方向</span><span class="metric-value" id="aCvdDesc">--</span></div>
        <div class="metric"><span class="metric-label">Delta</span><span class="metric-value" id="aDelta">--</span></div>
        <div class="metric"><span class="metric-label">价格位置</span><span class="metric-value" id="aPricePos" style="font-size:11px;">--</span></div>
        <div class="metric"><span class="metric-label">成交动能</span><span class="metric-value" id="aSpeed">--</span></div>
        <div style="margin-top:12px;font-size:13px;font-weight:bold;color:#888;margin-bottom:8px;">📍 关键价位</div>
        <div class="metric"><span class="metric-label">POC</span><span class="metric-value" id="aPoc">--</span></div>
        <div class="metric"><span class="metric-label">VAH</span><span class="metric-value" id="aVah">--</span></div>
        <div class="metric"><span class="metric-label">VAL</span><span class="metric-value" id="aVal">--</span></div>
        <div class="metric"><span class="metric-label">价格 vs POC</span><span class="metric-value" id="aPocDist">--</span></div>
      </div>
      <!-- 右: 信号详情 -->
      <div>
        <div style="font-size:13px;font-weight:bold;color:#888;margin-bottom:8px;">🎯 活跃信号</div>
        <div id="aSignalList" style="min-height:60px;font-size:12px;">等待数据...</div>
        <div style="margin-top:12px;">
          <div class="metric"><span class="metric-label">多方信号</span><span class="metric-value positive" id="aBullCount">0</span></div>
          <div class="metric"><span class="metric-label">空方信号</span><span class="metric-value negative" id="aBearCount">0</span></div>
          <div class="metric"><span class="metric-label">共识</span><span class="metric-value" id="aConsensus">--</span></div>
          <div class="metric"><span class="metric-label">置信度</span><span class="metric-value" id="aConfidence">--</span></div>
        </div>
        <div style="margin-top:12px;padding:8px;background:#0d0d14;border-radius:4px;font-size:11px;color:#666;">
          ⏱ 每 2 分钟自动更新分析<br>
          📊 基于订单流引擎 9 个模块综合判断
        </div>
      </div>
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
  document.getElementById('clock').textContent = new Date().toISOString().substring(11,19) + ' UTC';
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

// === 走势分析（每 2 分钟刷新）===
function fmtPrice(n) { return n ? '$' + Number(n).toLocaleString(undefined, {maximumFractionDigits:1}) : '--'; }

function updateAnalysis(d) {
  if (!d || d.error) return;
  
  // 时间
  document.getElementById('analysisTime').textContent = d.time_str || '--';
  
  // 评分
  const scoreEl = document.getElementById('analysisScore');
  scoreEl.textContent = d.score;
  scoreEl.style.color = d.score >= 55 ? '#00d4aa' : d.score <= 45 ? '#ff4757' : '#ffa502';
  document.getElementById('analysisOutlook').textContent = d.outlook;
  document.getElementById('analysisOutlook').style.color = scoreEl.style.color;
  
  // 建议
  const recBox = document.getElementById('analysisRecommendation');
  const recDir = document.getElementById('recDirection');
  recDir.textContent = d.recommendation.direction;
  recDir.style.color = d.recommendation.direction_en === 'LONG' ? '#00d4aa' : d.recommendation.direction_en === 'SHORT' ? '#ff4757' : '#ffa502';
  recBox.style.borderColor = recDir.style.color;
  recBox.style.background = d.recommendation.direction_en === 'LONG' ? 'rgba(0,212,170,0.1)' : d.recommendation.direction_en === 'SHORT' ? 'rgba(255,71,87,0.1)' : 'rgba(255,165,2,0.1)';
  document.getElementById('recReason').textContent = d.recommendation.reason;
  
  // 入场/止损/止盈
  if (d.recommendation.direction_en !== 'WAIT') {
    document.getElementById('recEntry').textContent = fmtPrice(d.recommendation.entry);
    document.getElementById('recSL').textContent = fmtPrice(d.recommendation.stop_loss);
    document.getElementById('recTP').textContent = fmtPrice(d.recommendation.take_profit);
    document.getElementById('recLevels').style.display = 'block';
  } else {
    document.getElementById('recLevels').style.display = 'none';
  }
  
  // 趋势
  const t = d.trend;
  const cvdEl = document.getElementById('aCvdDesc');
  cvdEl.textContent = t.cvd_desc;
  cvdEl.className = 'metric-value ' + (t.cvd > 0 ? 'positive' : t.cvd < 0 ? 'negative' : 'neutral');
  
  const deltaEl = document.getElementById('aDelta');
  deltaEl.textContent = (t.delta >= 0 ? '+' : '') + t.delta.toFixed(1);
  deltaEl.className = 'metric-value ' + (t.delta >= 0 ? 'positive' : 'negative');
  
  document.getElementById('aPricePos').textContent = t.price_pos;
  document.getElementById('aSpeed').textContent = t.speed;
  
  // 关键价位
  const lv = d.levels;
  document.getElementById('aPoc').textContent = fmtPrice(lv.poc);
  document.getElementById('aVah').textContent = fmtPrice(lv.vah);
  document.getElementById('aVal').textContent = fmtPrice(lv.val);
  const pocDistEl = document.getElementById('aPocDist');
  pocDistEl.textContent = (lv.poc_dist_pct >= 0 ? '+' : '') + lv.poc_dist_pct.toFixed(3) + '%';
  pocDistEl.className = 'metric-value ' + (lv.poc_dist_pct > 0 ? 'positive' : lv.poc_dist_pct < 0 ? 'negative' : 'neutral');
  
  // 信号
  const sig = d.signals;
  let sigHtml = '';
  (sig.active || []).forEach(s => {
    const isBull = s.includes('看多');
    const isBear = s.includes('看空');
    const cls = isBull ? 'signal-bullish' : isBear ? 'signal-bearish' : 'signal-neutral';
    const icon = isBull ? '🟢' : isBear ? '🔴' : '⚪';
    sigHtml += '<div class="signal-item ' + cls + '">' + icon + ' ' + s + '</div>';
  });
  document.getElementById('aSignalList').innerHTML = sigHtml || '<div style="color:#666">暂无活跃信号</div>';
  
  document.getElementById('aBullCount').textContent = sig.bullish;
  document.getElementById('aBearCount').textContent = sig.bearish;
  
  const consEl = document.getElementById('aConsensus');
  const consMap = {bullish:'🟢 做多', bearish:'🔴 做空', neutral:'⚪ 观望'};
  consEl.textContent = consMap[sig.consensus] || sig.consensus;
  consEl.className = 'metric-value ' + (sig.consensus === 'bullish' ? 'positive' : sig.consensus === 'bearish' ? 'negative' : 'neutral');
  
  document.getElementById('aConfidence').textContent = sig.confidence + '%';
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
