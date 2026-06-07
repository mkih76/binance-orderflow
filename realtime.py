#!/usr/bin/env python3
"""
实时订单流交易引擎 (WebSocket)
================================
通过 WebSocket 实时接收币安合约数据，逐笔分析订单流

数据流:
  aggTrade → 逐笔成交（买/卖、价格、数量、时间）
  depth    → 订单簿深度（买卖挂单）
  markPrice → 标记价格（资金费率）

架构:
  WebSocket → 数据缓冲 → 订单流引擎 → 信号生成 → 交易执行
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
import socket

socks.set_default_proxy(socks.SOCKS5, "127.0.0.1", 1080)
socket.socket = socks.socksocket

import websocket

sys.path.insert(0, "/opt/binance-testnet")
from orderflow import (
    FootprintChart, DeltaTracker, VolumeProfile,
    ImbalanceDetector, AbsorptionDetector, ExhaustionDetector,
    IcebergDetector, SpeedOfTape, OrderFlowSignalEngine
)

# ==================== 配置 ====================

WS_CONFIG = {
    "symbol": "btcusdt",
    "aggtrade_stream": "wss://fstream.binancefuture.com/ws/btcusdt@aggTrade",
    "depth_stream": "wss://fstream.binancefuture.com/ws/btcusdt@depth20@100ms",
    "mark_stream": "wss://fstream.binancefuture.com/ws/btcusdt@markPrice@1s",
    
    # 聚合流端点
    "combined_stream": "wss://fstream.binancefuture.com/stream?streams=btcusdt@aggTrade/btcusdt@depth20@100ms/btcusdt@markPrice@1s",
    
    # 重连
    "reconnect_delay": 5,
    "max_reconnect": 10,
    
    # 分析
    "tick_size": 1.0,
    "analysis_interval": 10,  # 每 10 秒分析一次
    "report_interval": 60,    # 每 60 秒打印报告
}

# ==================== 实时数据管理器 ====================

class RealtimeDataManager:
    """实时数据管理器"""
    
    def __init__(self):
        self.trades = deque(maxlen=5000)        # 最近 5000 笔成交
        self.depth = {"bids": [], "asks": []}    # 订单簿
        self.mark_price = None
        self.funding_rate = None
        self.last_update = 0
        self.trade_count = 0
        self.msg_per_sec = 0
        self._msg_counter = 0
        self._counter_reset = time.time()
        
        # 延迟统计
        self.latencies = deque(maxlen=100)
        self.avg_latency = 0
    
    def add_trade(self, data):
        """添加实时成交"""
        now = time.time()
        
        trade = {
            "a": data["a"],                    # aggregate trade ID
            "p": data["p"],                    # price
            "q": data["q"],                    # quantity
            "f": data.get("f", 0),             # first trade ID
            "l": data.get("l", 0),             # last trade ID
            "T": data["T"],                    # timestamp
            "m": data["m"],                    # is buyer maker
        }
        self.trades.append(trade)
        self.trade_count += 1
        self.last_update = now
        
        # 计算延迟（服务器时间 vs 本地时间）
        server_ts = data["T"] / 1000
        latency = (now - server_ts) * 1000  # ms
        if 0 < latency < 10000:  # 合理范围
            self.latencies.append(latency)
            self.avg_latency = sum(self.latencies) / len(self.latencies)
        
        # 消息速率
        self._msg_counter += 1
        if now - self._counter_reset >= 1.0:
            self.msg_per_sec = self._msg_counter
            self._msg_counter = 0
            self._counter_reset = now
    
    def update_depth(self, data):
        """更新订单簿"""
        self.depth["bids"] = [(float(p), float(q)) for p, q in data.get("bids", [])]
        self.depth["asks"] = [(float(p), float(q)) for p, q in data.get("asks", [])]
    
    def update_mark(self, data):
        """更新标记价格"""
        self.mark_price = float(data.get("p", 0))
        self.funding_rate = float(data.get("r", 0))
    
    def get_recent_trades(self, n=1000):
        """获取最近 N 笔成交"""
        return list(self.trades)[-n:]
    
    def get_bid_ask(self):
        """获取最优买卖价"""
        bid = self.depth["bids"][0][0] if self.depth["bids"] else 0
        ask = self.depth["asks"][0][0] if self.depth["asks"] else 0
        return bid, ask
    
    def get_book_imbalance(self):
        """
        订单簿失衡
        
        bid_vol / (bid_vol + ask_vol)
        > 0.6 = 买方挂单多（支撑强）
        < 0.4 = 卖方挂单多（压力大）
        """
        bid_vol = sum(q for _, q in self.depth["bids"][:10])
        ask_vol = sum(q for _, q in self.depth["asks"][:10])
        total = bid_vol + ask_vol
        if total == 0:
            return 0.5
        return bid_vol / total

# ==================== WebSocket 管理器 ====================

class BinanceWSManager:
    """币安 WebSocket 连接管理"""
    
    def __init__(self, data_manager, config=None):
        self.dm = data_manager
        self.cfg = config or WS_CONFIG
        self.ws = None
        self.running = False
        self.reconnect_count = 0
        self._lock = threading.Lock()
        
        # 回调
        self.on_trade_callbacks = []
        self.on_depth_callbacks = []
        self.on_signal_callbacks = []
    
    def on_message(self, ws, message):
        """处理收到的消息"""
        try:
            data = json.loads(message)
            
            # 聚合流格式
            if "stream" in data:
                stream = data["stream"]
                payload = data["data"]
            else:
                stream = ""
                payload = data
            
            event = payload.get("e", "")
            
            if event == "aggTrade":
                self.dm.add_trade(payload)
                for cb in self.on_trade_callbacks:
                    cb(payload)
            
            elif event == "depthUpdate":
                # combined stream 用 b/a，单独流用 bids/asks
                bids = payload.get("bids", payload.get("b", []))
                asks = payload.get("asks", payload.get("a", []))
                self.dm.depth["bids"] = [(float(p), float(q)) for p, q in bids]
                self.dm.depth["asks"] = [(float(p), float(q)) for p, q in asks]
                for cb in self.on_depth_callbacks:
                    cb(payload)
            
            elif event == "markPriceUpdate":
                self.dm.update_mark(payload)
        
        except Exception as e:
            pass  # 忽略解析错误
    
    def on_error(self, ws, error):
        print(f"  ❌ WS 错误: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        print(f"  ⚠️ WS 连接关闭: {close_status_code} {close_msg}")
        self.running = False
    
    def on_open(self, ws):
        print(f"  ✅ WS 连接建立")
        self.reconnect_count = 0
    
    def connect(self):
        """建立 WebSocket 连接"""
        url = self.cfg["combined_stream"]
        
        print(f"  🔌 连接: {url[:60]}...")
        
        self.ws = websocket.WebSocketApp(
            url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        
        self.running = True
        
        # 在独立线程运行
        self._thread = threading.Thread(
            target=self.ws.run_forever,
            kwargs={"ping_interval": 20, "ping_timeout": 10},
            daemon=True,
        )
        self._thread.start()
    
    def reconnect(self):
        """重连"""
        self.reconnect_count += 1
        if self.reconnect_count > self.cfg["max_reconnect"]:
            print(f"  ❌ 超过最大重连次数，退出")
            return False
        
        delay = self.cfg["reconnect_delay"]
        print(f"  🔄 {delay}秒后重连 (第{self.reconnect_count}次)...")
        time.sleep(delay)
        self.connect()
        return True
    
    def disconnect(self):
        """断开连接"""
        self.running = False
        if self.ws:
            self.ws.close()

# ==================== 实时分析引擎 ====================

class RealtimeAnalyzer:
    """实时订单流分析"""
    
    def __init__(self, data_manager, tick_size=1.0):
        self.dm = data_manager
        self.engine = OrderFlowSignalEngine(tick_size=tick_size)
        self.last_analysis = 0
        self.last_report = 0
        self.last_trade_count = 0
        self.signals_history = deque(maxlen=100)
    
    def update(self, force=False):
        """增量更新分析"""
        now = time.time()
        
        # 只在有新数据时分析
        new_trades = self.dm.trade_count - self.last_trade_count
        if new_trades < 10 and not force:
            return None
        
        # 获取新成交
        trades = self.dm.get_recent_trades(2000)
        if not trades:
            return None
        
        # 喂入引擎（只喂新数据）
        self.engine.feed(trades)
        
        # 运行分析
        signals = self.engine.analyze(trades)
        
        self.last_analysis = now
        self.last_trade_count = self.dm.trade_count
        
        return signals
    
    def generate_trading_signal(self, signals):
        """从信号生成交易指令"""
        if not signals:
            return None
        
        consensus, confidence = self.engine.get_consensus()
        vah, val, poc = self.engine.volume_profile.get_value_area()
        
        bid, ask = self.dm.get_bid_ask()
        mid_price = (bid + ask) / 2 if bid and ask else poc
        book_imb = self.dm.get_book_imbalance()
        
        if not mid_price:
            return None
        
        # 需要至少 2 个同方向信号
        bullish = [s for s in signals if s.get("bias") == "bullish"]
        bearish = [s for s in signals if s.get("bias") == "bearish"]
        
        trading_signal = None
        
        # 做多条件
        if len(bullish) >= 2:
            sl = mid_price * 0.9935  # 0.65% 止损
            tp = mid_price * 1.013   # 1.3% 止盈 (2R)
            
            # 订单簿确认
            if book_imb > 0.55:  # 买方挂单多
                confidence_boost = 0.1
            else:
                confidence_boost = 0
            
            trading_signal = {
                "direction": "long",
                "entry": mid_price,
                "stop_loss": sl,
                "take_profit": tp,
                "confidence": confidence + confidence_boost,
                "sources": [s.get("source", "?") for s in bullish],
                "consensus": consensus,
                "poc": poc,
                "vah": vah,
                "val": val,
                "book_imbalance": book_imb,
                "timestamp": time.time(),
            }
        
        # 做空条件
        elif len(bearish) >= 2:
            sl = mid_price * 1.0065
            tp = mid_price * 0.987
            
            if book_imb < 0.45:
                confidence_boost = 0.1
            else:
                confidence_boost = 0
            
            trading_signal = {
                "direction": "short",
                "entry": mid_price,
                "stop_loss": sl,
                "take_profit": tp,
                "confidence": confidence + confidence_boost,
                "sources": [s.get("source", "?") for s in bearish],
                "consensus": consensus,
                "poc": poc,
                "vah": vah,
                "val": val,
                "book_imbalance": book_imb,
                "timestamp": time.time(),
            }
        
        if trading_signal:
            self.signals_history.append(trading_signal)
        
        return trading_signal
    
    def print_realtime_status(self):
        """打印实时状态"""
        now = time.time()
        bid, ask = self.dm.get_bid_ask()
        mid = (bid + ask) / 2 if bid and ask else 0
        spread = ask - bid if bid and ask else 0
        
        _, _, poc = self.engine.volume_profile.get_value_area()
        poc_str = f"{poc:,.0f}" if poc else "---"
        
        delta = self.engine.delta
        
        print(f"\r  💰 {mid:,.1f} | "
              f"Bid:{bid:,.1f} Ask:{ask:,.1f} Spread:{spread:.1f} | "
              f"CVD:{delta.cvd:+,.0f} | "
              f"Trades:{self.dm.trade_count} ({self.dm.msg_per_sec}/s) | "
              f"Latency:{self.dm.avg_latency:.0f}ms | "
              f"POC:{poc_str}", end="", flush=True)

# ==================== 主程序 ====================

def run_realtime(dry_run=True):
    """运行实时订单流系统"""
    
    print("="*60)
    print("  🔴 实时订单流交易系统")
    print(f"  模式: {'🧪 干跑' if dry_run else '🚀 实盘'}")
    print("="*60)
    
    # 初始化
    dm = RealtimeDataManager()
    ws_mgr = BinanceWSManager(dm, WS_CONFIG)
    analyzer = RealtimeAnalyzer(dm, tick_size=1.0)
    
    # 连接
    print("\n📡 建立 WebSocket 连接...")
    ws_mgr.connect()
    
    # 等待连接建立
    for i in range(10):
        if ws_mgr.running:
            break
        time.sleep(1)
    
    if not ws_mgr.running:
        print("❌ 连接失败")
        return
    
    # 等待初始数据
    print("⏳ 等待数据流入...")
    for i in range(15):
        if dm.trade_count > 50:
            break
        time.sleep(1)
    
    print(f"✅ 收到 {dm.trade_count} 笔成交")
    
    # 主循环
    print("\n🔄 开始实时分析...")
    print("  按 Ctrl+C 停止\n")
    
    last_report = 0
    last_signal_check = 0
    active_position = None
    
    try:
        while ws_mgr.running:
            now = time.time()
            
            # 检查重连
            if not ws_mgr.running:
                if not ws_mgr.reconnect():
                    break
                time.sleep(3)
                continue
            
            # 增量分析（每 10 秒）
            if now - last_signal_check >= WS_CONFIG["analysis_interval"]:
                signals = analyzer.update()
                last_signal_check = now
                
                # 生成交易信号
                if signals:
                    ts = analyzer.generate_trading_signal(signals)
                    
                    if ts:
                        icon = "🟢" if ts["direction"] == "long" else "🔴"
                        sources = ", ".join(ts["sources"])
                        
                        print(f"\n  {'='*55}")
                        print(f"  {icon} 交易信号: {ts['direction'].upper()}")
                        print(f"  {'='*55}")
                        print(f"  入场价: {ts['entry']:,.1f}")
                        print(f"  止损:   {ts['stop_loss']:,.1f} ({abs(ts['entry']-ts['stop_loss'])/ts['entry']*100:.2f}%)")
                        print(f"  止盈:   {ts['take_profit']:,.1f} ({abs(ts['take_profit']-ts['entry'])/ts['entry']*100:.2f}%)")
                        print(f"  来源:   {sources}")
                        print(f"  共识:   {ts['consensus']} ({ts['confidence']:.0%})")
                        print(f"  POC:    {ts['poc']:,.0f}  VAH: {ts['vah']:,.0f}  VAL: {ts['val']:,.0f}")
                        print(f"  订单簿: 买方占比 {ts['book_imbalance']:.1%}")
                        print(f"  {'='*55}")
                        
                        if not dry_run and not active_position:
                            # 实盘下单
                            from trader import place_order
                            side = "BUY" if ts["direction"] == "long" else "SELL"
                            result = place_order("BTCUSDT", side, "MARKET", 0.005)
                            if result:
                                active_position = ts
                                print(f"  ✅ 开仓成功!")
                        elif dry_run:
                            print(f"  🧪 [干跑] 模拟开仓")
            
            # 实时状态行（每秒更新）
            analyzer.print_realtime_status()
            
            # 定期报告（每 60 秒）
            if now - last_report >= WS_CONFIG["report_interval"]:
                print(f"\n\n  📊 定期报告 ({datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC)")
                print(f"  {'─'*50}")
                
                # Delta/CVD
                analyzer.engine.delta.print_status()
                
                # Volume Profile
                vah, val, poc = analyzer.engine.volume_profile.get_value_area()
                if poc:
                    print(f"\n  📍 关键价位: POC={poc:,.0f} VAH={vah:,.0f} VAL={val:,.0f}")
                
                # 速度
                analyzer.engine.speed.print_status()
                
                # 信号统计
                recent_signals = list(analyzer.signals_history)[-10:]
                if recent_signals:
                    bullish_count = sum(1 for s in recent_signals if s["direction"] == "long")
                    bearish_count = sum(1 for s in recent_signals if s["direction"] == "short")
                    print(f"\n  📈 近期信号: 多={bullish_count} 空={bearish_count}")
                
                # 连接状态
                print(f"\n  🔗 连接状态: {'✅ 在线' if ws_mgr.running else '❌ 断开'}")
                print(f"  📡 总成交: {dm.trade_count} 笔 | {dm.msg_per_sec}/秒")
                print(f"  ⏱️ 平均延迟: {dm.avg_latency:.0f}ms")
                print(f"  {'─'*50}\n")
                
                last_report = now
            
            time.sleep(0.1)  # 100ms 刷新率
    
    except KeyboardInterrupt:
        print("\n\n🛑 用户中断")
    
    finally:
        ws_mgr.disconnect()
        print("🔌 WebSocket 已断开")
        
        # 保存最终状态
        analyzer.print_realtime_status()
        print("\n\n📊 最终统计:")
        print(f"  总成交: {dm.trade_count} 笔")
        print(f"  总信号: {len(analyzer.signals_history)} 个")
        print(f"  平均延迟: {dm.avg_latency:.0f}ms")

# ==================== 入口 ====================

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv or len(sys.argv) < 2
    run_realtime(dry_run=dry_run)
