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
from datetime import datetime, timezone, timedelta

BJT = timezone(timedelta(hours=8))  # 北京时间

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
from orderflow import (
    fetch_aggtrades, fetch_aggtrades_full, fetch_klines, fetch_depth,
    FootprintChart, DeltaTracker, VolumeProfile,
    ImbalanceDetector, AbsorptionDetector, ExhaustionDetector,
    IcebergDetector, SpeedOfTape, OrderFlowSignalEngine,
    MarketRegimeDetector, ATRCalculator, BookImbalance,
    MultiTimeframeConfirm, OIFundingAnalyzer, AdaptiveParams,
    DynamicPositionSizer, TradeJournal, EnhancedSignalEngine,
    MarketReasoning, auto_tick_size,
)
from trader import (
    place_order, get_positions, get_balance,
    get_price, api_request, get_base_url, get_current_mode,
    place_stop_order, place_take_profit_order, cancel_all_orders,
    is_position_active, set_leverage
)

# ==================== 策略参数 ====================

CONFIG = {
    "symbol": "BTCUSDT",
    "tick_size": 1.0,

    # 仓位（动态管理，这些是基础值）
    "default_qty": 0.005,       # 默认仓位 BTC
    "conservative_qty": 0.003,  # 保守仓位
    "min_qty": 0.001,           # 最小仓位
    "max_qty": 0.01,            # 最大仓位
    "leverage": 3,              # 杠杆
    "account_balance": 100,     # 账户余额 USDT

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

    # 止盈止损（自适应参数会动态调整这些值）
    "stop_loss_pct": 0.0065,    # 止损 0.65%
    "take_profit_1r": 0.0065,   # 1R 止盈
    "take_profit_2r": 0.013,    # 2R 止盈
    "trailing_stop_1r": True,   # 盈利 1R 后移动止损

    # 交易时段 (北京时间) — 0-24 = 全天候
    "active_hours": (0, 24),

    # 数据
    "trade_lookback": 1000,     # 回看成交笔数
    "kline_interval": "5m",     # K线周期
    "kline_limit": 50,          # K线数量

    # 增强模块开关
    "use_enhanced_engine": True,        # 使用增强版引擎
    "use_dynamic_position": True,       # 使用动态仓位
    "use_adaptive_params": True,        # 使用自适应参数
    "use_mtf_confirm": True,            # 使用多时间框架确认
    "use_book_imbalance": True,         # 使用订单簿失衡
    "use_oi_funding": True,             # 使用 OI + 资金费率
}

# ==================== 策略状态 ====================

STATE_FILE = os.path.join(BASE_DIR, "strategy_state.json")

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

def generate_signals(engine, trades, cfg, enhanced_result=None):
    """
    从订单流引擎生成交易信号

    Args:
        engine: OrderFlowSignalEngine 或 EnhancedSignalEngine
        trades: aggTrades
        cfg: 配置参数
        enhanced_result: 增强版分析结果（可选）

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

    # 使用自适应参数（如果可用）
    stop_pct = cfg["stop_loss_pct"]
    tp_pct = cfg["take_profit_1r"]
    if enhanced_result and cfg.get("use_adaptive_params"):
        ap = enhanced_result.get("adaptive_params", {})
        stop_pct = ap.get("stop_loss_pct", stop_pct)
        tp_pct = ap.get("take_profit_pct", tp_pct)

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
            sl_dist = current_price * stop_pct
            tp_dist = current_price * tp_pct

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
            sl_dist = current_price * stop_pct
            tp_dist = current_price * tp_pct

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
            sl_dist = current_price * stop_pct

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
    if val and abs(current_price - val) / val < 0.002:
        sell_pressure = sum(1 for s in all_signals if s.get("bias") == "bearish")
        if sell_pressure >= 2 and consensus != "bearish":
            sl = current_price - current_price * stop_pct
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
            sl = current_price + current_price * stop_pct
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

    # ---- 信号 5: 订单簿失衡确认（增强模块）----
    if enhanced_result and cfg.get("use_book_imbalance"):
        book = enhanced_result.get("book", {})
        book_bias = book.get("bias", "neutral")
        if book_bias != "neutral":
            direction = "long" if book_bias == "bullish" else "short"
            # 订单簿信号只作为已有信号的确认，不独立产生入场信号
            # 但可以提升已有信号的置信度
            for s in signals:
                if s.direction == direction:
                    s.confidence = min(0.95, s.confidence + 0.1)
                    s.reason += f" + DOM{'买方' if direction=='long' else '卖方'}支撑"

    return signals

# ==================== 风控检查 ====================

def risk_check(state, cfg):
    """
    风控检查

    daily_pnl 存储的是绝对金额（USDT），不是百分比。
    每笔交易的盈亏 = pnl_pct / 100 * account_balance
    """
    today = datetime.now(BJT).strftime("%Y-%m-%d")

    # 重置每日计数
    if state["last_trade_date"] != today:
        state["daily_trades"] = 0
        state["daily_pnl"] = 0.0
        state["last_trade_date"] = today

    # 检查交易次数
    if state["daily_trades"] >= cfg["max_daily_trades"]:
        return False, "已达今日最大交易次数"

    # 检查日亏损（基于账户余额的绝对金额）
    max_loss_amount = cfg["account_balance"] * cfg["max_daily_loss"]
    if state["daily_pnl"] <= -max_loss_amount:
        return False, f"已达今日最大亏损 {max_loss_amount:.1f} USDT ({cfg['max_daily_loss']*100:.0f}%)"

    # 检查连亏
    if state["consecutive_losses"] >= cfg["max_consecutive_loss"]:
        return False, f"连续亏损 {state['consecutive_losses']} 次，停手"

    # 检查交易时段（北京时间）
    bj_hour = datetime.now(BJT).hour
    start_h, end_h = cfg["active_hours"]
    if not (start_h <= bj_hour < end_h):
        return False, f"不在交易时段 ({start_h}:00-{end_h}:00 北京时间)"

    # 检查是否已有持仓
    if state.get("open_position"):
        return False, "已有持仓"

    return True, "OK"

# ==================== 策略主循环 ====================

def run_strategy(dry_run=False):
    """运行策略（增强版）"""
    cfg = CONFIG
    state = load_state()

    # 初始化引擎
    # 获取初始价格以自动选择 tick_size
    init_price = get_price(cfg["symbol"])
    if init_price:
        tick = auto_tick_size(init_price)
    else:
        tick = cfg["tick_size"]
    print(f"  tick_size: {tick} (基于价格 {init_price or 'N/A'})")

    if cfg.get("use_enhanced_engine"):
        engine = EnhancedSignalEngine(tick_size=tick)
        print(f"{'🧪 干跑模式' if dry_run else '🚀 实盘模式'} (增强版引擎)")
    else:
        engine = OrderFlowSignalEngine(tick_size=tick)
        print(f"{'🧪 干跑模式' if dry_run else '🚀 实盘模式'} (基础引擎)")

    # 初始化交易日志
    journal = TradeJournal() if cfg.get("use_enhanced_engine") else None

    # 初始化 AI 终审官
    ai_reasoner = MarketReasoning()

    # 初始化动态仓位管理
    sizer = DynamicPositionSizer(
        account_balance=cfg["account_balance"],
        risk_per_trade=cfg["risk_per_trade"],
        base_qty=cfg["default_qty"],
        min_qty=cfg["min_qty"],
        max_qty=cfg["max_qty"],
        leverage=cfg["leverage"],
    ) if cfg.get("use_dynamic_position") else None

    print(f"标的: {cfg['symbol']}  本金: {cfg['account_balance']} USDT  杠杆: {cfg['leverage']}x")
    print(f"仓位: {'动态' if sizer else cfg['default_qty']} BTC  止损: {'自适应' if cfg.get('use_adaptive_params') else str(cfg['stop_loss_pct']*100)+'%'}")
    print(f"{'='*50}")

    cycle = 0
    while True:
        cycle += 1
        now = datetime.now(BJT)
        print(f"\n--- 周期 {cycle} | {now.strftime('%H:%M:%S')} 北京时间 ---")

        try:
            # 1. 采集数据
            trades = fetch_aggtrades_full(cfg["symbol"], minutes=5)
            if not trades:
                print("  ⚠️ 无法获取数据")
                time.sleep(30)
                continue

            # 2. 喂入引擎
            engine.feed(trades)

            # 3. 运行分析
            enhanced_result = None
            if isinstance(engine, EnhancedSignalEngine):
                depth_data = fetch_depth(cfg["symbol"]) if cfg.get("use_book_imbalance") else None
                enhanced_result = engine.analyze_enhanced(trades, depth_data, cfg["symbol"])

                # 打印市场状态
                regime = enhanced_result["regime"]
                mtf = enhanced_result["mtf"]
                print(f"  📊 市场: {regime['regime']} ({regime['confidence']:.0%}) - {regime['details']}")
                print(f"  📊 多时间框架: {mtf['details']}")
                print(f"  📊 ATR: {enhanced_result['atr_pct']*100:.2f}%")

            # 4. 生成信号
            signals = generate_signals(engine, trades, cfg, enhanced_result)

            # 5. 风控检查
            can_trade, reason = risk_check(state, cfg)

            # 6. 获取当前价格
            current_price = float(trades[-1]["p"])

            # 7. 检查持仓止损/止盈
            if state.get("open_position"):
                pos = state["open_position"]
                entry = pos["entry_price"]
                sl = pos["stop_loss"]
                tp = pos["take_profit"]
                direction = pos["direction"]
                hold_time = (time.time() - pos["entry_time"]) / 60

                # --- OCO 检查：服务端止损/止盈单是否已触发 ---
                # 如果持仓已被服务端单平掉，取消另一个挂单，更新状态
                if not dry_run and not is_position_active(cfg["symbol"]):
                    # 持仓已消失，说明服务端 SL 或 TP 已触发
                    cancel_all_orders(cfg["symbol"])  # 撤掉另一个挂单
                    # 估算 PnL（用当前价近似）
                    pnl = (current_price - entry) if direction == "long" else (entry - current_price)
                    pnl_pct = pnl / entry * 100
                    pnl_usdt = pnl * pos["qty"]
                    # 判断是止损还是止盈触发的
                    if (direction == "long" and current_price <= sl) or \
                       (direction == "short" and current_price >= sl):
                        exit_reason = "stop_loss"
                        icon = "🔴"
                        state["consecutive_losses"] += 1
                    elif (direction == "long" and current_price >= tp) or \
                         (direction == "short" and current_price <= tp):
                        exit_reason = "take_profit"
                        icon = "🟢"
                        state["consecutive_losses"] = 0
                        state["total_wins"] += 1
                    else:
                        # 价格在 SL 和 TP 之间，可能是服务端用标记价触发的
                        exit_reason = "server_triggered"
                        icon = "⚡"
                    print(f"  {icon} 服务端{exit_reason}已触发! 入场={entry:.1f} 现价={current_price:.1f} PnL={pnl_pct:+.2f}% ({pnl_usdt:+.2f} USDT)")
                    trade_id = pos.get("trade_id", f"T{state['total_trades']}")
                    if journal:
                        journal.log_exit(trade_id, current_price, exit_reason, pnl, pnl_pct, hold_time)
                    state["daily_pnl"] += pnl_usdt
                    state["open_position"] = None
                    state["daily_trades"] += 1
                    save_state(state)
                    continue

                # 止损
                if (direction == "long" and current_price <= sl) or \
                   (direction == "short" and current_price >= sl):
                    pnl = (current_price - entry) if direction == "long" else (entry - current_price)
                    pnl_pct = pnl / entry * 100
                    pnl_usdt = pnl * pos["qty"]  # 实际盈亏金额
                    print(f"  🔴 止损触发! 入场={entry:.1f} 现价={current_price:.1f} PnL={pnl_pct:+.2f}% ({pnl_usdt:+.2f} USDT)")

                    if journal:
                        trade_id = pos.get("trade_id", f"T{state['total_trades']}")
                        journal.log_exit(trade_id, current_price, "stop_loss", pnl, pnl_pct, hold_time)

                    if not dry_run:
                        cancel_all_orders(cfg["symbol"])
                        close_side = "SELL" if direction == "long" else "BUY"
                        close_result = place_order(cfg["symbol"], close_side, "MARKET", pos["qty"])
                        if close_result and journal:
                            journal.log_order(trade_id, close_result, "short" if direction=="long" else "long", pos["qty"], current_price, "close")

                    state["daily_pnl"] += pnl_usdt  # 累加绝对金额
                    state["consecutive_losses"] += 1
                    state["open_position"] = None
                    state["daily_trades"] += 1
                    save_state(state)
                    continue

                # 止盈 — 分批止盈：2R 平一半 + 移止损到 1R，3R 全平
                risk = abs(entry - sl)
                if direction == "long":
                    tp_2r = entry + risk * 2  # 2R 价位
                    tp_3r = entry + risk * 3  # 3R 价位
                else:
                    tp_2r = entry - risk * 2
                    tp_3r = entry - risk * 3

                # 3R 全平
                if (direction == "long" and current_price >= tp_3r) or \
                   (direction == "short" and current_price <= tp_3r):
                    pnl = (current_price - entry) if direction == "long" else (entry - current_price)
                    pnl_pct = pnl / entry * 100
                    pnl_usdt = pnl * pos["qty"]
                    print(f"  🟢 3R止盈! 入场={entry:.1f} 现价={current_price:.1f} PnL={pnl_pct:+.2f}% ({pnl_usdt:+.2f} USDT)")

                    if journal:
                        trade_id = pos.get("trade_id", f"T{state['total_trades']}")
                        journal.log_exit(trade_id, current_price, "take_profit_3r", pnl, pnl_pct, hold_time)

                    if not dry_run:
                        cancel_all_orders(cfg["symbol"])
                        close_side = "SELL" if direction == "long" else "BUY"
                        close_result = place_order(cfg["symbol"], close_side, "MARKET", pos["qty"])
                        if close_result and journal:
                            journal.log_order(trade_id, close_result, "short" if direction=="long" else "long", pos["qty"], current_price, "close")

                    state["daily_pnl"] += pnl_usdt
                    state["consecutive_losses"] = 0
                    state["open_position"] = None
                    state["daily_trades"] += 1
                    state["total_wins"] += 1
                    save_state(state)
                    continue

                # 2R 平一半 + 移止损到 1R（只执行一次）
                if not pos.get("partial_closed") and \
                   ((direction == "long" and current_price >= tp_2r) or \
                    (direction == "short" and current_price <= tp_2r)):
                    half_qty = round(pos["qty"] / 2, 4)
                    if half_qty >= 0.001:  # 至少最小下单量
                        pnl = (current_price - entry) if direction == "long" else (entry - current_price)
                        pnl_pct = pnl / entry * 100
                        pnl_usdt = pnl * half_qty
                        print(f"  🟢 2R止盈! 平一半 {half_qty} BTC @ {current_price:.1f} PnL={pnl_pct:+.2f}% ({pnl_usdt:+.2f} USDT)")

                        if journal:
                            trade_id = pos.get("trade_id", f"T{state['total_trades']}")
                            journal.log_exit(trade_id, current_price, "take_profit_2r_half", pnl, pnl_pct, hold_time)

                        if not dry_run:
                            cancel_all_orders(cfg["symbol"])
                            close_side = "SELL" if direction == "long" else "BUY"
                            close_result = place_order(cfg["symbol"], close_side, "MARKET", half_qty)
                            if close_result and journal:
                                journal.log_order(trade_id, close_result, "short" if direction=="long" else "long", half_qty, current_price, "close")

                        # 更新状态：减仓 + 止损移到 1R
                        new_qty = round(pos["qty"] - half_qty, 4)
                        if direction == "long":
                            new_sl = entry + risk * 0.5  # 1R 位置（保本+0.5R）
                        else:
                            new_sl = entry - risk * 0.5

                        state["open_position"]["qty"] = new_qty
                        state["open_position"]["partial_closed"] = True
                        state["open_position"]["stop_loss"] = new_sl
                        state["daily_pnl"] += pnl_usdt

                        # 重新下止损单（用新数量和新止损价）
                        if not dry_run:
                            close_side = "SELL" if direction == "long" else "BUY"
                            sl_order = place_stop_order(cfg["symbol"], close_side, new_sl, new_qty)
                            if sl_order:
                                state["open_position"]["sl_order_id"] = sl_order.get("orderId")

                        save_state(state)
                        print(f"  📍 剩余仓位 {new_qty} BTC，止损移到 {new_sl:.1f} (1R)")
                        continue

                # 超时平仓
                if hold_time > cfg["max_hold_minutes"]:
                    pnl = (current_price - entry) if direction == "long" else (entry - current_price)
                    pnl_pct = pnl / entry * 100
                    pnl_usdt = pnl * pos["qty"]
                    print(f"  ⏰ 超时平仓! 持仓 {hold_time:.0f} 分钟, PnL={pnl_pct:+.2f}% ({pnl_usdt:+.2f} USDT)")

                    if journal:
                        trade_id = pos.get("trade_id", f"T{state['total_trades']}")
                        journal.log_exit(trade_id, current_price, "timeout", pnl, pnl_pct, hold_time)

                    if not dry_run:
                        cancel_all_orders(cfg["symbol"])
                        close_side = "SELL" if direction == "long" else "BUY"
                        close_result = place_order(cfg["symbol"], close_side, "MARKET", pos["qty"])
                        if close_result and journal:
                            journal.log_order(trade_id, close_result, "short" if direction=="long" else "long", pos["qty"], current_price, "close")

                    state["daily_pnl"] += pnl_usdt
                    state["open_position"] = None
                    state["daily_trades"] += 1
                    save_state(state)
                    continue

                # 追踪止损（每次价格创新高/新低都跟随上移）
                if cfg["trailing_stop_1r"]:
                    risk = abs(entry - sl)
                    # 盈利达到 1R 后启动追踪
                    if direction == "long" and current_price >= entry + risk:
                        # 追踪止损 = 当前价格 - 0.5R，且只能上移不能下移
                        new_sl = current_price - risk * 0.5
                        if new_sl > sl:
                            state["open_position"]["stop_loss"] = new_sl
                            # 记录最高价用于追踪
                            state["open_position"]["trail_high"] = max(
                                state["open_position"].get("trail_high", entry), current_price)
                            print(f"  📍 追踪止损到 {new_sl:.1f} (最高价 {current_price:.1f})")
                            if not dry_run:
                                cancel_all_orders(cfg["symbol"])
                                close_side = "SELL" if direction == "long" else "BUY"
                                sl_order = place_stop_order(cfg["symbol"], close_side, new_sl, pos["qty"])
                                if sl_order:
                                    state["open_position"]["sl_order_id"] = sl_order.get("orderId")
                            save_state(state)
                    elif direction == "short" and current_price <= entry - risk:
                        new_sl = current_price + risk * 0.5
                        if new_sl < sl:
                            state["open_position"]["stop_loss"] = new_sl
                            state["open_position"]["trail_low"] = min(
                                state["open_position"].get("trail_low", entry), current_price)
                            print(f"  📍 追踪止损到 {new_sl:.1f} (最低价 {current_price:.1f})")
                            if not dry_run:
                                cancel_all_orders(cfg["symbol"])
                                close_side = "SELL" if direction == "long" else "BUY"
                                sl_order = place_stop_order(cfg["symbol"], close_side, new_sl, pos["qty"])
                                if sl_order:
                                    state["open_position"]["sl_order_id"] = sl_order.get("orderId")
                            save_state(state)

                print(f"  📊 持仓中: {direction} @ {entry:.1f} | SL={state['open_position']['stop_loss']:.1f} TP={tp:.1f} | {hold_time:.0f}min")

            # 8. 寻找入场机会
            elif signals and can_trade:
                signals.sort(key=lambda s: s.confidence, reverse=True)
                best = signals[0]

                # 获取自适应参数的最小信号数
                min_signals = cfg["min_signal_agreement"]
                if enhanced_result and cfg.get("use_adaptive_params"):
                    min_signals = enhanced_result["adaptive_params"].get("min_signals", min_signals)

                same_dir = [s for s in signals if s.direction == best.direction]

                if len(same_dir) >= min_signals:
                    # === AI 终审：规则信号通过后，AI 有一票否决权 ===
                    ai_verdict = None
                    ai_report = None
                    try:
                        vah_val, val_val, poc_val = engine.volume_profile.get_value_area()
                        # 计算 ATR
                        prices = [float(t["p"]) for t in trades[-200:]]
                        atr_pct_val = 0.0
                        if len(prices) > 14:
                            trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
                            atr_val = sum(trs[-14:]) / 14
                            atr_pct_val = atr_val / current_price * 100 if current_price else 0

                        ai_report = ai_reasoner.analyze_with_ai(
                            price=current_price,
                            poc=poc_val, vah=vah_val, val=val_val,
                            cvd=engine.delta.cvd, delta=engine.delta.current_delta,
                            signals=signals,
                            trades=trades,
                            depth_bids=None, depth_asks=None,
                            atr_pct=atr_pct_val,
                        )
                        if ai_report and not ai_report.get("error"):
                            ai_verdict = ai_report.get("verdict", "WAIT")
                            ai_conf = ai_report.get("confidence", 0)
                            print(f"  🧠 AI 终审: {ai_verdict} ({ai_conf}%)")

                            # AI 否决：方向不一致或建议观望
                            if ai_verdict in ("WAIT", "AVOID"):
                                print(f"  🚫 AI 否决: {ai_verdict} — 不开仓")
                                continue
                            elif ai_verdict == "LONG" and best.direction != "long":
                                print(f"  🚫 AI 否决: 信号做空但 AI 看多 — 不开仓")
                                continue
                            elif ai_verdict == "SHORT" and best.direction != "short":
                                print(f"  🚫 AI 否决: 信号做多但 AI 看空 — 不开仓")
                                continue
                        else:
                            print(f"  ⚠️ AI 未返回有效结果，规则引擎放行")
                    except Exception as e:
                        print(f"  ⚠️ AI 终审异常: {e}，规则引擎放行")

                    # 动态仓位计算
                    qty = cfg["default_qty"]
                    if sizer and cfg.get("use_dynamic_position"):
                        regime = enhanced_result["regime"]["regime"] if enhanced_result else "ranging"
                        qty_mult = enhanced_result["adaptive_params"]["qty_multiplier"] if enhanced_result else 1.0
                        qty = sizer.calculate(
                            entry_price=current_price,
                            stop_loss_pct=cfg["stop_loss_pct"],
                            signal_count=len(same_dir),
                            regime=regime,
                            qty_multiplier=qty_mult,
                        )

                    print(f"  🎯 入场信号: {best.direction.upper()} @ {best.entry_price:.1f}")
                    print(f"     来源: {best.source} ({best.reason})")
                    print(f"     SL={best.stop_loss:.1f} TP={best.take_profit:.1f}")
                    print(f"     一致性: {len(same_dir)} 个信号  仓位: {qty} BTC")

                    # 记录交易日志
                    trade_id = f"T{state['total_trades'] + 1}"
                    if journal:
                        signal_details = [{"source": s.source, "direction": s.direction, "confidence": s.confidence} for s in same_dir]
                        regime_info = enhanced_result["regime"] if enhanced_result else {"regime": "unknown"}
                        journal.log_entry(
                            trade_id=trade_id,
                            direction=best.direction,
                            entry_price=current_price,
                            qty=qty,
                            stop_loss=best.stop_loss,
                            take_profit=best.take_profit,
                            signals=signal_details,
                            regime=regime_info,
                            params=enhanced_result["adaptive_params"] if enhanced_result else {},
                            reason=best.reason,
                        )
                        journal.log_signal_snapshot(
                            trade_id=trade_id,
                            price=current_price,
                            cvd=engine.delta.cvd,
                            delta=engine.delta.current_delta,
                            poc=engine.volume_profile.get_poc(),
                            vah=engine.volume_profile.get_value_area()[0],
                            val=engine.volume_profile.get_value_area()[1],
                            consensus=enhanced_result["consensus"] if enhanced_result else "unknown",
                            signals=[s.source for s in same_dir],
                        )

                    if not dry_run:
                        # 确保杠杆与策略配置一致
                        set_leverage(cfg["symbol"], cfg["leverage"])
                        side = "BUY" if best.direction == "long" else "SELL"
                        result = place_order(cfg["symbol"], side, "MARKET", qty)

                        if result:
                            if journal:
                                journal.log_order(trade_id, result, best.direction, qty, current_price, "entry")
                            state["open_position"] = {
                                "direction": best.direction,
                                "entry_price": current_price,
                                "stop_loss": best.stop_loss,
                                "take_profit": best.take_profit,
                                "qty": qty,
                                "entry_time": time.time(),
                                "source": best.source,
                                "reason": best.reason,
                                "trade_id": trade_id,
                            }
                            state["total_trades"] += 1
                            save_state(state)

                            # 下服务端止损/止盈单
                            close_side = "SELL" if best.direction == "long" else "BUY"
                            sl_order = place_stop_order(cfg["symbol"], close_side, best.stop_loss, qty)
                            tp_order = place_take_profit_order(cfg["symbol"], close_side, best.take_profit, qty)
                            if sl_order:
                                state["open_position"]["sl_order_id"] = sl_order.get("orderId")
                            if tp_order:
                                state["open_position"]["tp_order_id"] = tp_order.get("orderId")
                            save_state(state)

                            print(f"  ✅ 开仓成功! 止损/止盈单已下到服务端")
                else:
                    print(f"  ⏳ 信号不够一致 ({len(same_dir)}/{min_signals})")

            elif not can_trade:
                print(f"  🚫 无法交易: {reason}")

            else:
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

            # 打印交易统计（如果有日志）
            if journal and cycle % 10 == 0:
                stats = journal.get_stats()
                if stats["total"] > 0:
                    print(f"\n  📈 交易统计: 胜率={stats['win_rate']:.0%} 总交易={stats['total']} 总PnL={stats['total_pnl']:+.2f}%")

        except Exception as e:
            print(f"  ❌ 错误: {e}")
            import traceback
            traceback.print_exc()

        # 等待下一个周期
        time.sleep(30)

# ==================== 入口 ====================

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    run_strategy(dry_run=dry_run)
