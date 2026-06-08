#!/usr/bin/env python3
"""
订单流策略回测框架
=================
从币安历史数据回放信号，模拟交易，统计绩效

用法:
    python backtest.py                    # 默认回测最近 2 小时
    python backtest.py --minutes 60       # 回测最近 60 分钟
    python backtest.py --data trades.json # 从本地文件回放
    python backtest.py --verbose          # 详细输出每笔交易
"""

import sys
import os
import time
import json
import argparse
from datetime import datetime, timezone, timedelta

BJT = timezone(timedelta(hours=8))
from collections import deque

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from orderflow import (
    fetch_aggtrades_full, OrderFlowSignalEngine, FootprintChart
)
from strategy import CONFIG, TradingSignal, generate_signals

# ==================== 回测引擎 ====================

class BacktestEngine:
    """回测引擎：重放历史数据，模拟交易"""
    
    def __init__(self, config=None, initial_capital=100.0, leverage=3):
        self.cfg = config or CONFIG
        self.initial_capital = initial_capital
        self.leverage = leverage
        self.capital = initial_capital
        
        # 交易记录
        self.trades = []
        self.equity_curve = []
        self.signals_log = []
        
        # 当前持仓
        self.position = None
        
        # 统计
        self.total_trades = 0
        self.wins = 0
        self.losses = 0
        self.total_pnl = 0.0
        self.max_equity = initial_capital
        self.max_drawdown = 0.0
        self.consecutive_losses = 0
        self.max_consecutive_losses = 0
    
    def run(self, trades_data, verbose=False):
        """
        回测主循环
        
        Args:
            trades_data: aggTrades 列表（按时间排序）
            verbose: 是否打印每笔交易
        """
        if not trades_data:
            print("❌ 无数据")
            return
        
        print(f"📊 回测数据: {len(trades_data)} 笔成交")
        print(f"⏰ 时间范围: {self._ts_str(trades_data[0]['T'])} → {self._ts_str(trades_data[-1]['T'])}")
        print(f"💰 初始资金: {self.initial_capital} USDT  杠杆: {self.leverage}x")
        print(f"{'='*60}")
        
        # 初始化引擎
        engine = OrderFlowSignalEngine(tick_size=self.cfg["tick_size"])
        
        # 按 5 分钟窗口分批喂入
        window_ms = 300 * 1000  # 5 分钟
        start_ts = trades_data[0]["T"]
        end_ts = trades_data[-1]["T"]
        
        current_ts = start_ts
        window_idx = 0
        
        while current_ts <= end_ts:
            window_end = current_ts + window_ms
            window_trades = [t for t in trades_data if current_ts <= t["T"] < window_end]
            
            if len(window_trades) < 10:
                current_ts = window_end
                continue
            
            # 喂入引擎
            engine.feed(window_trades)
            
            # 生成信号
            signals = generate_signals(engine, window_trades, self.cfg)
            
            current_price = float(window_trades[-1]["p"])
            current_time = window_trades[-1]["T"]
            
            # 检查持仓止损/止盈
            if self.position:
                self._check_position(current_price, current_time, verbose)
            
            # 寻找入场
            if not self.position and signals:
                same_dir = {}
                for s in signals:
                    same_dir.setdefault(s.direction, []).append(s)
                
                for direction, sigs in same_dir.items():
                    if len(sigs) >= self.cfg["min_signal_agreement"]:
                        best = max(sigs, key=lambda s: s.confidence)
                        self._open_position(best, current_price, current_time, verbose)
                        break
            
            # 记录权益
            unrealized = 0
            if self.position:
                entry = self.position["entry_price"]
                qty = self.position["qty"]
                if self.position["direction"] == "long":
                    unrealized = (current_price - entry) * qty * self.leverage
                else:
                    unrealized = (entry - current_price) * qty * self.leverage
            
            equity = self.capital + unrealized
            self.equity_curve.append({"time": current_time, "equity": equity})
            self.max_equity = max(self.max_equity, equity)
            dd = (self.max_equity - equity) / self.max_equity
            self.max_drawdown = max(self.max_drawdown, dd)
            
            current_ts = window_end
            window_idx += 1
        
        # 强制平仓
        if self.position:
            last_price = float(trades_data[-1]["p"])
            self._close_position(last_price, trades_data[-1]["T"], "回测结束", verbose)
        
        self._print_report()
    
    def _open_position(self, signal, price, timestamp, verbose):
        """开仓"""
        # 计算仓位（基于风险百分比）
        risk_amount = self.capital * self.cfg["risk_per_trade"]
        sl_distance = price * self.cfg["stop_loss_pct"]
        qty = risk_amount / sl_distance if sl_distance > 0 else self.cfg["default_qty"]
        qty = min(qty, self.cfg["default_qty"])  # 不超过默认仓位
        
        self.position = {
            "direction": signal.direction,
            "entry_price": price,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "qty": qty,
            "entry_time": timestamp,
            "source": signal.source,
        }
        
        if verbose:
            icon = "🟢" if signal.direction == "long" else "🔴"
            print(f"  {icon} 开仓: {signal.direction.upper()} @ {price:.1f} qty={qty:.4f}")
            print(f"     SL={signal.stop_loss:.1f} TP={signal.take_profit:.1f} [{signal.source}]")
    
    def _close_position(self, price, timestamp, reason, verbose):
        """平仓"""
        pos = self.position
        entry = pos["entry_price"]
        qty = pos["qty"]
        
        if pos["direction"] == "long":
            pnl = (price - entry) * qty * self.leverage
        else:
            pnl = (entry - price) * qty * self.leverage
        
        pnl_pct = pnl / self.capital * 100
        self.capital += pnl
        self.total_pnl += pnl
        
        self.total_trades += 1
        if pnl > 0:
            self.wins += 1
            self.consecutive_losses = 0
        else:
            self.losses += 1
            self.consecutive_losses += 1
            self.max_consecutive_losses = max(self.max_consecutive_losses, self.consecutive_losses)
        
        trade_record = {
            "direction": pos["direction"],
            "entry": entry,
            "exit": price,
            "qty": qty,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "source": pos["source"],
            "reason": reason,
            "duration_ms": timestamp - pos["entry_time"],
        }
        self.trades.append(trade_record)
        
        if verbose:
            icon = "✅" if pnl > 0 else "❌"
            print(f"  {icon} 平仓: {pos['direction']} @ {price:.1f} PnL={pnl:+.2f} ({pnl_pct:+.2f}%) [{reason}]")
        
        self.position = None
    
    def _check_position(self, price, timestamp, verbose):
        """检查止损/止盈"""
        pos = self.position
        direction = pos["direction"]
        sl = pos["stop_loss"]
        tp = pos["take_profit"]
        entry = pos["entry_price"]
        
        # 止损
        if (direction == "long" and price <= sl) or \
           (direction == "short" and price >= sl):
            self._close_position(sl, timestamp, "止损", verbose)
            return
        
        # 止盈
        if (direction == "long" and price >= tp) or \
           (direction == "short" and price <= tp):
            self._close_position(tp, timestamp, "止盈", verbose)
            return
        
        # 超时（30 分钟）
        hold_ms = timestamp - pos["entry_time"]
        if hold_ms > self.cfg["max_hold_minutes"] * 60 * 1000:
            self._close_position(price, timestamp, "超时", verbose)
            return
        
        # 移动止损
        if self.cfg["trailing_stop_1r"]:
            risk = abs(entry - sl)
            if direction == "long" and price >= entry + risk:
                new_sl = entry + risk * 0.5
                if new_sl > sl:
                    pos["stop_loss"] = new_sl
            elif direction == "short" and price <= entry - risk:
                new_sl = entry - risk * 0.5
                if new_sl < sl:
                    pos["stop_loss"] = new_sl
    
    def _ts_str(self, ts_ms):
        return datetime.fromtimestamp(ts_ms / 1000, tz=BJT).strftime("%H:%M:%S")
    
    def _print_report(self):
        """打印回测报告"""
        print(f"\n{'='*60}")
        print(f"  回测报告")
        print(f"{'='*60}")
        
        if self.total_trades == 0:
            print("  ⚠️ 无交易记录")
            return
        
        win_rate = self.wins / self.total_trades * 100
        avg_win = sum(t["pnl"] for t in self.trades if t["pnl"] > 0) / max(1, self.wins)
        avg_loss = sum(t["pnl"] for t in self.trades if t["pnl"] <= 0) / max(1, self.losses)
        profit_factor = abs(avg_win * self.wins / (avg_loss * self.losses)) if avg_loss != 0 else float("inf")
        
        # Sharpe ratio（简化：假设无风险利率=0）
        returns = [t["pnl_pct"] for t in self.trades]
        if len(returns) > 1:
            mean_ret = sum(returns) / len(returns)
            std_ret = (sum((r - mean_ret) ** 2 for r in returns) / len(returns)) ** 0.5
            sharpe = (mean_ret / std_ret) * (len(returns) ** 0.5) if std_ret > 0 else 0
        else:
            sharpe = 0
        
        print(f"\n  📊 交易统计:")
        print(f"     总交易: {self.total_trades}")
        print(f"     盈利: {self.wins}  亏损: {self.losses}")
        print(f"     胜率: {win_rate:.1f}%")
        print(f"     盈亏比: {profit_factor:.2f}")
        
        print(f"\n  💰 资金统计:")
        print(f"     初始资金: {self.initial_capital:.2f} USDT")
        print(f"     最终资金: {self.capital:.2f} USDT")
        print(f"     总盈亏: {self.total_pnl:+.2f} USDT ({self.total_pnl/self.initial_capital*100:+.1f}%)")
        print(f"     最大回撤: {self.max_drawdown*100:.1f}%")
        print(f"     Sharpe Ratio: {sharpe:.2f}")
        
        print(f"\n  📈 平均盈亏:")
        print(f"     平均盈利: {avg_win:+.4f} USDT")
        print(f"     平均亏损: {avg_loss:+.4f} USDT")
        print(f"     最大连亏: {self.max_consecutive_losses} 次")
        
        # 信号来源统计
        source_stats = {}
        for t in self.trades:
            src = t["source"]
            if src not in source_stats:
                source_stats[src] = {"wins": 0, "losses": 0, "pnl": 0}
            if t["pnl"] > 0:
                source_stats[src]["wins"] += 1
            else:
                source_stats[src]["losses"] += 1
            source_stats[src]["pnl"] += t["pnl"]
        
        print(f"\n  🎯 信号来源分析:")
        for src, stats in sorted(source_stats.items(), key=lambda x: x[1]["pnl"], reverse=True):
            total = stats["wins"] + stats["losses"]
            wr = stats["wins"] / total * 100 if total > 0 else 0
            print(f"     {src}: {total}笔 胜率={wr:.0f}% PnL={stats['pnl']:+.2f}")
        
        # 最大单笔盈亏
        if self.trades:
            best = max(self.trades, key=lambda t: t["pnl"])
            worst = min(self.trades, key=lambda t: t["pnl"])
            print(f"\n  🏆 最大单笔盈利: {best['pnl']:+.2f} ({best['direction']} [{best['source']}])")
            print(f"  💀 最大单笔亏损: {worst['pnl']:+.2f} ({worst['direction']} [{worst['source']}])")
        
        print(f"\n{'='*60}")
    
    def save_results(self, path="backtest_results.json"):
        """保存回测结果到 JSON"""
        results = {
            "config": {k: v for k, v in self.cfg.items() if isinstance(v, (int, float, str, bool, tuple))},
            "initial_capital": self.initial_capital,
            "final_capital": self.capital,
            "total_pnl": self.total_pnl,
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": self.wins / max(1, self.total_trades),
            "max_drawdown": self.max_drawdown,
            "trades": self.trades,
            "equity_curve": self.equity_curve[::10],  # 每 10 个点采样
        }
        with open(os.path.join(BASE_DIR, path), "w") as f:
            json.dump(results, f, indent=2)
        print(f"💾 结果已保存: {path}")


# ==================== 入口 ====================

def main():
    parser = argparse.ArgumentParser(description="订单流策略回测")
    parser.add_argument("--minutes", type=int, default=120, help="回测最近 N 分钟的数据")
    parser.add_argument("--data", type=str, help="从本地 JSON 文件加载数据")
    parser.add_argument("--verbose", action="store_true", help="详细输出每笔交易")
    parser.add_argument("--save", type=str, default="backtest_results.json", help="保存结果路径")
    args = parser.parse_args()
    
    # 加载数据
    if args.data:
        print(f"📂 从文件加载: {args.data}")
        with open(args.data) as f:
            trades_data = json.load(f)
    else:
        print(f"📡 从币安获取最近 {args.minutes} 分钟数据...")
        trades_data = fetch_aggtrades_full("BTCUSDT", minutes=args.minutes)
    
    if not trades_data:
        print("❌ 无数据")
        return
    
    # 运行回测
    engine = BacktestEngine(config=CONFIG, initial_capital=100.0, leverage=3)
    engine.run(trades_data, verbose=args.verbose)
    engine.save_results(args.save)


if __name__ == "__main__":
    main()
