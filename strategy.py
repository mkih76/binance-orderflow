#!/usr/bin/env python3
"""
订单流自动交易策略
==================
基于 TRADING_PLAN.md 实现的自动做单系统

用法:
    python strategy.py                  # 运行策略（默认 BTCUSDT）
    python strategy.py --dry-run        # 干跑模式（只看信号不下单）
    python strategy.py --backtest       # 回测模式
"""

import sys
import time
import json
import os
from datetime import datetime, timezone

sys.path.insert(0, "/opt/binance-testnet")
from orderflow import (
    fetch_aggtrades, fetch_klines, fetch_depth,
    FootprintChart, DeltaTracker, VolumeProfile,
    ImbalanceDetector, AbsorptionDetector, ExhaustionDetector,
    IcebergDetector, SpeedOfTape, OrderFlowSignalEngine
)
from trader import (
    place_order, get_positions, get_balance,
    get_price, api_request, get_base_url, get_current_mode
)

# ==================== 策略参数 ====================

CONFIG = {
    "symbol": "BTCUSDT",
    "tick_size": 1.0,
    
    # 仓位
    "default_qty": 0.005,       # 默认仓位 BTC
    "conservative_qty": 0.003,  # 保守仓位
    "min_qty": 0.001,           # 最小仓位
    
    # 风控
    "risk_per_trade": 0.02,     # 每笔风险 2%
    "max_daily_loss": 0.05,     # 日最大亏损 5%
    "max_daily_trades": 3,      # 日最大交易次数
    "max_consecutive_loss": 2,  # 连亏停手
    "max_hold_minutes": 30,     # 最大持仓时间
    
    # 信号
    "min_signal_agreement": 2,  # 至少 2 个信号一致才入场
    "imbalance_ratio": 3.0,     # 堆叠失衡比率
    "imbalance_min_stack": 3,   # 最少堆叠层数
    "absorption_z": 3.0,        # 吸收 Z-Score 阈值
    "absorption_imbalance": 0.6,# 吸收失衡阈值
    "absorption_impact": 0.8,   # 吸收价格影响阈值
    
    # 止盈止损
    "stop_loss_pct": 0.0065,    # 止损 0.65%
    "take_profit_1r": 0.0065,   # 1R 止盈
    "take_profit_2r": 0.013,    # 2R 止盈
    "trailing_stop_1r": True,   # 盈利 1R 后移动止损
    
    # 交易时段 (UTC) — 0-24 = 全天候
    "active_hours": (0, 24),
    
    # 数据
    "trade_lookback": 1000,     # 回看成交笔数
    "kline_interval": "5m",     # K线周期
    "kline_limit": 50,          # K线数量
}

# ==================== 策略状态 ====================

STATE_FILE = "/opt/binance-testnet/strategy_state.json"

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "daily_trades": 0,
            "daily_pnl": 0.0,
            "consecutive_losses": 0,
            "last_trade_time": 0,
            "last_trade_date": "",
            "total_trades": 0,
            "total_wins": 0,
            "total_pnl": 0.0,
            "open_position": None,
        }

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ==================== 信号生成 ====================

class TradingSignal:
    """交易信号"""
    def __init__(self, direction, source, confidence, entry_price, stop_loss, take_profit, reason):
        self.direction = direction  # "long" / "short"
        self.source = source
        self.confidence = confidence
        self.entry_price = entry_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.reason = reason
        self.timestamp = time.time()
    
    def __repr__(self):
        return f"Signal({self.direction} @ {self.entry_price:.1f}, SL={self.stop_loss:.1f}, TP={self.take_profit:.1f}, src={self.source})"

def generate_signals(engine, trades, cfg):
    """
    从订单流引擎生成交易信号
    
    Returns:
        list of TradingSignal
    """
    signals = []
    
    # 运行分析
    all_signals = engine.analyze(trades)
    consensus, confidence = engine.get_consensus()
    
    # 获取关键价位
    vah, val, poc = engine.volume_profile.get_value_area()
    if not poc:
        return signals
    
    current_price = float(trades[-1]["p"]) if trades else poc
    
    # ---- 信号 1: 堆叠失衡 + 速度确认 ----
    imbalance_signals = [s for s in all_signals if s.get("source") == "stacked_imbalance"]
    speed_momentum = engine.speed.get_momentum()
    
    for imb in imbalance_signals:
        direction = "long" if imb["bias"] == "bullish" else "short"
        
        # 速度确认
        speed_confirms = (
            (direction == "long" and speed_momentum == "accelerating_buy") or
            (direction == "short" and speed_momentum == "accelerating_sell")
        )
        
        if speed_confirms and imb.get("levels", 0) >= cfg["imbalance_min_stack"]:
            sl_dist = current_price * cfg["stop_loss_pct"]
            tp_dist = current_price * cfg["take_profit_1r"]
            
            if direction == "long":
                sl = current_price - sl_dist
                tp = current_price + tp_dist * 2  # 2R
            else:
                sl = current_price + sl_dist
                tp = current_price - tp_dist * 2
            
            signals.append(TradingSignal(
                direction=direction,
                source="stacked_imbalance",
                confidence=0.7,
                entry_price=current_price,
                stop_loss=sl,
                take_profit=tp,
                reason=f"堆叠{imb['levels']}层{'买' if direction=='long' else '卖'}方失衡 + 速度{'加速' if speed_confirms else ''}"
            ))
    
    # ---- 信号 2: 吸收反转 ----
    abs_signals = [s for s in all_signals if s.get("source") == "absorption"]
    
    for ab in abs_signals:
        # 吸收出现在关键价位附近
        near_key_level = (
            abs(current_price - poc) / poc < 0.002 or
            abs(current_price - vah) / vah < 0.002 or
            abs(current_price - val) / val < 0.002
        )
        
        if near_key_level:
            direction = "long" if ab["direction"] == "seller_absorption" else "short"
            sl_dist = current_price * cfg["stop_loss_pct"]
            tp_dist = current_price * cfg["take_profit_1r"]
            
            if direction == "long":
                sl = current_price - sl_dist
                tp = poc if poc > current_price else current_price + tp_dist * 2
            else:
                sl = current_price + sl_dist
                tp = poc if poc < current_price else current_price - tp_dist * 2
            
            signals.append(TradingSignal(
                direction=direction,
                source="absorption",
                confidence=0.8,
                entry_price=current_price,
                stop_loss=sl,
                take_profit=tp,
                reason=f"吸收事件: Z={ab.get('z_score', 0):.1f}, 失衡={ab.get('net_imbalance', 0):.0%}, 价格不动"
            ))
    
    # ---- 信号 3: CVD 背离 + 衰竭 ----
    div = engine.delta.get_divergence(lookback=10)
    exh_signals = [s for s in all_signals if s.get("source") == "exhaustion"]
    
    if div != "none":
        # 背离 + 在关键价位
        near_va = (
            abs(current_price - vah) / vah < 0.003 or
            abs(current_price - val) / val < 0.003
        )
        
        if near_va:
            direction = "long" if div == "bullish_div" else "short"
            sl_dist = current_price * cfg["stop_loss_pct"]
            
            if direction == "long":
                sl = current_price - sl_dist
                tp = poc  # 回归 POC
            else:
                sl = current_price + sl_dist
                tp = poc
            
            signals.append(TradingSignal(
                direction=direction,
                source="cvd_divergence",
                confidence=0.6,
                entry_price=current_price,
                stop_loss=sl,
                take_profit=tp,
                reason=f"CVD {'看涨' if direction=='long' else '看跌'}背离 + VA附近"
            ))
    
    # ---- 信号 4: 大户吸收 + 价值区边缘 ----
    # 价格在 VAL 附近 + 卖方被吸收 → 做多
    # 价格在 VAH 附近 + 买方被吸收 → 做空
    if val and abs(current_price - val) / val < 0.002:
        sell_pressure = sum(1 for s in all_signals if s.get("bias") == "bearish")
        if sell_pressure >= 2 and consensus != "bearish":
            # 卖压大但价格没跌 → 买方在吸收
            sl = current_price - current_price * cfg["stop_loss_pct"]
            tp = poc
            signals.append(TradingSignal(
                direction="long",
                source="val_bounce",
                confidence=0.65,
                entry_price=current_price,
                stop_loss=sl,
                take_profit=tp,
                reason=f"VAL({val:.0f})附近卖压被吸收"
            ))
    
    if vah and abs(current_price - vah) / vah < 0.002:
        buy_pressure = sum(1 for s in all_signals if s.get("bias") == "bullish")
        if buy_pressure >= 2 and consensus != "bullish":
            sl = current_price + current_price * cfg["stop_loss_pct"]
            tp = poc
            signals.append(TradingSignal(
                direction="short",
                source="vah_rejection",
                confidence=0.65,
                entry_price=current_price,
                stop_loss=sl,
                take_profit=tp,
                reason=f"VAH({vah:.0f})附近买压被吸收"
            ))
    
    return signals

# ==================== 风控检查 ====================

def risk_check(state, cfg):
    """风控检查"""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # 重置每日计数
    if state["last_trade_date"] != today:
        state["daily_trades"] = 0
        state["daily_pnl"] = 0.0
        state["last_trade_date"] = today
    
    # 检查交易次数
    if state["daily_trades"] >= cfg["max_daily_trades"]:
        return False, "已达今日最大交易次数"
    
    # 检查日亏损
    if state["daily_pnl"] <= -cfg["max_daily_loss"] * 100:
        return False, f"已达今日最大亏损 {cfg['max_daily_loss']*100:.0f}%"
    
    # 检查连亏
    if state["consecutive_losses"] >= cfg["max_consecutive_loss"]:
        return False, f"连续亏损 {state['consecutive_losses']} 次，停手"
    
    # 检查交易时段
    utc_hour = datetime.now(timezone.utc).hour
    start_h, end_h = cfg["active_hours"]
    if not (start_h <= utc_hour < end_h):
        return False, f"不在交易时段 ({start_h}:00-{end_h}:00 UTC)"
    
    # 检查是否已有持仓
    if state.get("open_position"):
        return False, "已有持仓"
    
    return True, "OK"

# ==================== 策略主循环 ====================

def run_strategy(dry_run=False):
    """运行策略"""
    cfg = CONFIG
    state = load_state()
    
    print(f"{'🧪 干跑模式' if dry_run else '🚀 实盘模式'}")
    print(f"标的: {cfg['symbol']}  本金: 100 USDT  杠杆: 3x")
    print(f"仓位: {cfg['default_qty']} BTC  止损: {cfg['stop_loss_pct']*100:.2f}%")
    print(f"{'='*50}")
    
    # 初始化引擎
    engine = OrderFlowSignalEngine(tick_size=cfg["tick_size"])
    
    cycle = 0
    while True:
        cycle += 1
        now = datetime.now(timezone.utc)
        print(f"\n--- 周期 {cycle} | {now.strftime('%H:%M:%S')} UTC ---")
        
        try:
            # 1. 采集数据
            trades = fetch_aggtrades(cfg["symbol"], limit=cfg["trade_lookback"])
            if not trades:
                print("  ⚠️ 无法获取数据")
                time.sleep(30)
                continue
            
            # 2. 喂入引擎
            engine.feed(trades)
            
            # 3. 生成信号
            signals = generate_signals(engine, trades, cfg)
            
            # 4. 风控检查
            can_trade, reason = risk_check(state, cfg)
            
            # 5. 获取当前价格和持仓
            current_price = float(trades[-1]["p"])
            positions = get_positions()
            
            # 6. 检查持仓止损/止盈
            if state.get("open_position"):
                pos = state["open_position"]
                entry = pos["entry_price"]
                sl = pos["stop_loss"]
                tp = pos["take_profit"]
                direction = pos["direction"]
                hold_time = (time.time() - pos["entry_time"]) / 60
                
                # 止损
                if (direction == "long" and current_price <= sl) or \
                   (direction == "short" and current_price >= sl):
                    pnl = (current_price - entry) if direction == "long" else (entry - current_price)
                    pnl_pct = pnl / entry * 100
                    print(f"  🔴 止损触发! 入场={entry:.1f} 现价={current_price:.1f} PnL={pnl_pct:+.2f}%")
                    
                    if not dry_run:
                        close_side = "SELL" if direction == "long" else "BUY"
                        place_order(cfg["symbol"], close_side, "MARKET", pos["qty"])
                    
                    state["daily_pnl"] += pnl_pct
                    state["consecutive_losses"] += 1
                    state["open_position"] = None
                    state["daily_trades"] += 1
                    save_state(state)
                    continue
                
                # 止盈
                if (direction == "long" and current_price >= tp) or \
                   (direction == "short" and current_price <= tp):
                    pnl = (current_price - entry) if direction == "long" else (entry - current_price)
                    pnl_pct = pnl / entry * 100
                    print(f"  🟢 止盈触发! 入场={entry:.1f} 现价={current_price:.1f} PnL={pnl_pct:+.2f}%")
                    
                    if not dry_run:
                        close_side = "SELL" if direction == "long" else "BUY"
                        place_order(cfg["symbol"], close_side, "MARKET", pos["qty"])
                    
                    state["daily_pnl"] += pnl_pct
                    state["consecutive_losses"] = 0
                    state["open_position"] = None
                    state["daily_trades"] += 1
                    state["total_wins"] += 1
                    save_state(state)
                    continue
                
                # 超时平仓
                if hold_time > cfg["max_hold_minutes"]:
                    pnl = (current_price - entry) if direction == "long" else (entry - current_price)
                    pnl_pct = pnl / entry * 100
                    print(f"  ⏰ 超时平仓! 持仓 {hold_time:.0f} 分钟, PnL={pnl_pct:+.2f}%")
                    
                    if not dry_run:
                        close_side = "SELL" if direction == "long" else "BUY"
                        place_order(cfg["symbol"], close_side, "MARKET", pos["qty"])
                    
                    state["daily_pnl"] += pnl_pct
                    state["open_position"] = None
                    state["daily_trades"] += 1
                    save_state(state)
                    continue
                
                # 移动止损（盈利 1R 后）
                if cfg["trailing_stop_1r"]:
                    risk = abs(entry - sl)
                    if direction == "long" and current_price >= entry + risk:
                        new_sl = entry + risk * 0.5  # 移到成本 + 0.5R
                        if new_sl > sl:
                            state["open_position"]["stop_loss"] = new_sl
                            print(f"  📍 移动止损到 {new_sl:.1f}")
                    elif direction == "short" and current_price <= entry - risk:
                        new_sl = entry - risk * 0.5
                        if new_sl < sl:
                            state["open_position"]["stop_loss"] = new_sl
                            print(f"  📍 移动止损到 {new_sl:.1f}")
                
                print(f"  📊 持仓中: {direction} @ {entry:.1f} | SL={state['open_position']['stop_loss']:.1f} TP={tp:.1f} | {hold_time:.0f}min")
            
            # 7. 寻找入场机会
            elif signals and can_trade:
                # 按置信度排序
                signals.sort(key=lambda s: s.confidence, reverse=True)
                best = signals[0]
                
                # 检查信号数量（至少 2 个一致方向）
                same_dir = [s for s in signals if s.direction == best.direction]
                
                if len(same_dir) >= cfg["min_signal_agreement"]:
                    print(f"  🎯 入场信号: {best.direction.upper()} @ {best.entry_price:.1f}")
                    print(f"     来源: {best.source} ({best.reason})")
                    print(f"     SL={best.stop_loss:.1f} TP={best.take_profit:.1f}")
                    print(f"     一致性: {len(same_dir)} 个信号")
                    
                    if not dry_run:
                        side = "BUY" if best.direction == "long" else "SELL"
                        result = place_order(cfg["symbol"], side, "MARKET", cfg["default_qty"])
                        
                        if result:
                            state["open_position"] = {
                                "direction": best.direction,
                                "entry_price": current_price,
                                "stop_loss": best.stop_loss,
                                "take_profit": best.take_profit,
                                "qty": cfg["default_qty"],
                                "entry_time": time.time(),
                                "source": best.source,
                                "reason": best.reason,
                            }
                            state["total_trades"] += 1
                            save_state(state)
                            print(f"  ✅ 开仓成功!")
                else:
                    print(f"  ⏳ 信号不够一致 ({len(same_dir)}/{cfg['min_signal_agreement']})")
            
            elif not can_trade:
                print(f"  🚫 无法交易: {reason}")
            
            else:
                # 打印当前状态
                consensus, conf = engine.get_consensus()
                vah, val, poc = engine.volume_profile.get_value_area()
                print(f"  价格: {current_price:.1f} | 共识: {consensus} ({conf:.0%})")
                if poc:
                    print(f"  POC={poc:.1f} VAH={vah:.1f} VAL={val:.1f}")
                if signals:
                    print(f"  信号: {', '.join(s.source for s in signals)}")
                print(f"  等待入场...")
            
            # 打印信号摘要
            if signals:
                print(f"\n  📋 活跃信号:")
                for s in signals:
                    icon = "🟢" if s.direction == "long" else "🔴"
                    print(f"    {icon} {s.source}: {s.direction} ({s.confidence:.0%}) - {s.reason}")
        
        except Exception as e:
            print(f"  ❌ 错误: {e}")
            import traceback
            traceback.print_exc()
        
        # 等待下一个周期
        time.sleep(30)  # 30 秒刷新

# ==================== 入口 ====================

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    run_strategy(dry_run=dry_run)
