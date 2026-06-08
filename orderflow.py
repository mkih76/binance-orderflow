#!/usr/bin/env python3
"""
订单流分析引擎 (Order Flow Engine)
==================================
ATAS 核心功能的 Python 实现，用于币安合约模拟盘

功能模块:
1. Footprint Chart - 足迹图（每个价格的买卖量）
2. Delta / CVD - 主动买卖差 / 累积量差
3. Volume Profile - 成交量分布（POC/VAH/VAL/HVN/LVN）
4. Stacked Imbalance - 堆叠失衡检测
5. Absorption - 吸收检测（大量成交但价格不动）
6. Exhaustion - 衰竭检测（量大但推不动价）
7. Iceberg Detection - 冰山单检测
8. Speed of Tape - 成交速度/动量
9. Trading Signals - 自动信号生成

增强模块:
10. MarketRegimeDetector - 市场状态识别（trending/ranging/breakout）
11. ATRCalculator - ATR 波动率计算
12. BookImbalance - 订单簿失衡检测
13. MultiTimeframeConfirm - 多时间框架确认
14. OIFundingAnalyzer - OI + 资金费率分析
15. AdaptiveParams - 自适应参数（基于 ATR）
16. DynamicPositionSizer - 动态仓位管理
17. TradeJournal - 交易日志系统
"""

import os
import time
import math
import requests
from collections import defaultdict, deque
from datetime import datetime

# ==================== 数据采集 ====================

def get_proxies():
    """SOCKS5 代理配置"""
    try:
        from config import PROXY_ENABLED, SOCKS5_PROXY
        if PROXY_ENABLED and SOCKS5_PROXY:
            return {"http": SOCKS5_PROXY, "https": SOCKS5_PROXY}
    except ImportError:
        pass
    return None

def fetch_aggtrades(symbol="BTCUSDT", limit=1000, start_time=None, end_time=None):
    """获取聚合成交数据"""
    base = "https://demo-fapi.binance.com"
    params = {"symbol": symbol, "limit": limit}
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time
    
    r = requests.get(f"{base}/fapi/v1/aggTrades", params=params,
                     proxies=get_proxies(), timeout=15)
    if r.status_code != 200:
        return []
    return r.json()

def fetch_aggtrades_full(symbol="BTCUSDT", minutes=30):
    """
    分页获取完整历史成交数据（突破 1000 笔限制）
    
    Args:
        symbol: 交易对
        minutes: 获取最近 N 分钟的数据
    
    Returns:
        list of aggTrades，按时间排序
    """
    all_trades = []
    end_time = int(time.time() * 1000)
    start_time = end_time - minutes * 60 * 1000
    base = "https://demo-fapi.binance.com"
    
    while start_time < end_time:
        params = {
            "symbol": symbol,
            "limit": 1000,
            "startTime": start_time,
            "endTime": end_time,
        }
        try:
            r = requests.get(f"{base}/fapi/v1/aggTrades", params=params,
                             proxies=get_proxies(), timeout=15)
            if r.status_code != 200:
                break
            batch = r.json()
            if not batch:
                break
            all_trades.extend(batch)
            # 下一批从最后一笔之后开始
            start_time = batch[-1]["T"] + 1
            time.sleep(0.1)  # 限流保护
        except Exception as e:
            print(f"⚠️ 数据采集异常: {e}")
            break
    
    return all_trades

def fetch_klines(symbol="BTCUSDT", interval="5m", limit=100):
    """获取 K 线数据"""
    base = "https://demo-fapi.binance.com"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(f"{base}/fapi/v1/klines", params=params,
                     proxies=get_proxies(), timeout=15)
    if r.status_code != 200:
        return []
    return r.json()

def fetch_depth(symbol="BTCUSDT", limit=20):
    """获取订单簿深度"""
    base = "https://demo-fapi.binance.com"
    params = {"symbol": symbol, "limit": limit}
    r = requests.get(f"{base}/fapi/v1/depth", params=params,
                     proxies=get_proxies(), timeout=15)
    if r.status_code != 200:
        return None
    return r.json()

def auto_tick_size(price):
    """根据价格自动选择合适的 tick_size"""
    if price >= 50000:
        return 10.0
    elif price >= 10000:
        return 5.0
    elif price >= 1000:
        return 1.0
    elif price >= 100:
        return 0.1
    else:
        return 0.01

# ==================== 1. 足迹图 (Footprint Chart) ====================

class FootprintChart:
    """
    足迹图：在每个价格水平上显示买/卖成交量
    
    原理：
    - 聚合成交中 isBuyerMaker=true → 卖方主动（taker卖）→ 记为 sell volume
    - 聚合成交中 isBuyerMaker=false → 买方主动（taker买）→ 记为 buy volume
    - 按价格水平聚合，形成每个价格的 bid×ask 矩阵
    """
    
    def __init__(self, tick_size=0.1):
        self.tick_size = tick_size  # 价格精度（BTCUSDT 合约约 0.1）
        self.bars = {}  # {bar_time: {price_level: {buy: x, sell: y}}}
    
    def _round_price(self, price):
        """将价格对齐到 tick"""
        return round(round(price / self.tick_size) * self.tick_size, 2)
    
    def add_trades(self, trades, bar_seconds=300):
        """
        将成交数据填充到足迹图
        
        Args:
            trades: aggTrades 列表
            bar_seconds: 每根K线的秒数（默认300=5分钟）
        """
        for t in trades:
            price = float(t["p"])
            qty = float(t["q"])
            timestamp = t["T"] / 1000  # ms → s
            is_sell = t["m"]  # isBuyerMaker=true → 卖方主动
            
            # 确定所属K线
            bar_time = int(timestamp // bar_seconds) * bar_seconds
            
            # 对齐价格
            price_level = self._round_price(price)
            
            if bar_time not in self.bars:
                self.bars[bar_time] = {}
            if price_level not in self.bars[bar_time]:
                self.bars[bar_time][price_level] = {"buy": 0.0, "sell": 0.0}
            
            if is_sell:
                self.bars[bar_time][price_level]["sell"] += qty
            else:
                self.bars[bar_time][price_level]["buy"] += qty
    
    def get_bar(self, bar_time):
        """获取指定K线的足迹数据"""
        return self.bars.get(bar_time, {})
    
    def get_latest_bar(self):
        """获取最新一根K线"""
        if not self.bars:
            return None, {}
        latest = max(self.bars.keys())
        return latest, self.bars[latest]
    
    def print_bar(self, bar_time=None, top_n=15):
        """打印足迹图"""
        if bar_time is None:
            bar_time, bar_data = self.get_latest_bar()
        else:
            bar_data = self.get_bar(bar_time)
        
        if not bar_data:
            print("  无数据")
            return
        
        # 按价格排序
        sorted_prices = sorted(bar_data.keys(), reverse=True)
        
        # 只显示成交量最大的 top_n 个价格
        if len(sorted_prices) > top_n:
            sorted_prices_by_vol = sorted(
                bar_data.keys(),
                key=lambda p: bar_data[p]["buy"] + bar_data[p]["sell"],
                reverse=True
            )[:top_n]
            sorted_prices = sorted(sorted_prices_by_vol, reverse=True)
        
        time_str = datetime.fromtimestamp(bar_time).strftime("%m-%d %H:%M") if bar_time else "N/A"
        print(f"\n{'='*55}")
        print(f"  足迹图 [{time_str}]")
        print(f"{'='*55}")
        print(f"  {'价格':>10}  {'买量(Bid)':>12}  {'卖量(Ask)':>12}  {'Delta':>10}  {'柱状图'}")
        print(f"  {'-'*10}  {'-'*12}  {'-'*12}  {'-'*10}  {'-'*20}")
        
        max_vol = max(
            (bar_data[p]["buy"] + bar_data[p]["sell"])
            for p in sorted_prices
        ) if sorted_prices else 1
        
        for price in sorted_prices:
            d = bar_data[price]
            buy = d["buy"]
            sell = d["sell"]
            delta = buy - sell
            total = buy + sell
            
            # 可视化
            buy_bar = "█" * max(1, int(buy / max_vol * 15))
            sell_bar = "░" * max(1, int(sell / max_vol * 15))
            delta_color = "+" if delta >= 0 else ""
            
            print(f"  {price:>10.1f}  {buy:>12.4f}  {sell:>12.4f}  {delta_color}{delta:>9.4f}  {buy_bar}{sell_bar}")

# ==================== 2. Delta / CVD ====================

class DeltaTracker:
    """
    Delta = 主动买量 - 主动卖量（每个时间段）
    CVD = Delta 的累积总和
    
    用途：
    - Delta > 0: 买方主导
    - Delta < 0: 卖方主导
    - CVD 与价格背离: 潜在反转信号
    """
    
    def __init__(self):
        self.history = []  # [(timestamp, delta, cvd, price)]
        self.cvd = 0.0
        self.current_delta = 0.0
        self.current_buy = 0.0
        self.current_sell = 0.0
        self._period_start = 0
    
    def add_trades(self, trades, period_seconds=60):
        """
        添加成交数据，按时间段计算 delta
        
        Args:
            trades: aggTrades 列表
            period_seconds: 每个 delta 计算周期（默认60秒）
        """
        for t in trades:
            qty = float(t["q"])
            timestamp = t["T"] / 1000
            is_sell = t["m"]
            price = float(t["p"])
            
            # 确定周期
            period = int(timestamp // period_seconds) * period_seconds
            
            if self._period_start == 0:
                self._period_start = period
            
            if period != self._period_start:
                # 保存当前周期
                self.history.append({
                    "time": self._period_start,
                    "delta": self.current_delta,
                    "cvd": self.cvd,
                    "buy": self.current_buy,
                    "sell": self.current_sell,
                    "price": price,
                })
                self.current_delta = 0.0
                self.current_buy = 0.0
                self.current_sell = 0.0
                self._period_start = period
            
            if is_sell:
                self.current_sell += qty
                self.current_delta -= qty
                self.cvd -= qty
            else:
                self.current_buy += qty
                self.current_delta += qty
                self.cvd += qty
        
        # Flush 最后一个周期（保存到历史但不重置，保持当前值可读）
        if trades and self._period_start > 0:
            # 检查是否已经有这个周期的历史记录
            if not self.history or self.history[-1]["time"] != self._period_start:
                self.history.append({
                    "time": self._period_start,
                    "delta": self.current_delta,
                    "cvd": self.cvd,
                    "buy": self.current_buy,
                    "sell": self.current_sell,
                    "price": float(trades[-1]["p"]),
                })
            else:
                # 更新已有记录
                self.history[-1] = {
                    "time": self._period_start,
                    "delta": self.current_delta,
                    "cvd": self.cvd,
                    "buy": self.current_buy,
                    "sell": self.current_sell,
                    "price": float(trades[-1]["p"]),
                }
    
    def get_divergence(self, lookback=10):
        """
        结构性 CVD 背离检测

        找到两个明确的低/高点，对比价格和 CVD 的结构：
        - 看涨背离: 价格走出 lower low，但 CVD 走出 higher low
        - 看跌背离: 价格走出 higher high，但 CVD 走出 lower high

        Returns:
            str: "bullish_div" / "bearish_div" / "none"
        """
        if len(self.history) < lookback:
            return "none"

        recent = self.history[-lookback:]
        prices = [h["price"] for h in recent]
        cvds = [h["cvd"] for h in recent]

        # 找两个局部低点（价格）
        lows = self._find_local_extremes(prices, mode="min")
        if len(lows) >= 2:
            p1_idx, p1_val = lows[-2]
            p2_idx, p2_val = lows[-1]
            c1_val = cvds[p1_idx]
            c2_val = cvds[p2_idx]
            # price lower low + CVD higher low → bullish divergence
            if p2_val < p1_val and c2_val > c1_val:
                return "bullish_div"

        # 找两个局部高点（价格）
        highs = self._find_local_extremes(prices, mode="max")
        if len(highs) >= 2:
            p1_idx, p1_val = highs[-2]
            p2_idx, p2_val = highs[-1]
            c1_val = cvds[p1_idx]
            c2_val = cvds[p2_idx]
            # price higher high + CVD lower high → bearish divergence
            if p2_val > p1_val and c2_val < c1_val:
                return "bearish_div"

        return "none"

    def _find_local_extremes(self, values, mode="min", window=2):
        """找局部极值点，返回 [(index, value), ...]"""
        extremes = []
        for i in range(window, len(values) - window):
            segment = values[i - window:i + window + 1]
            if mode == "min" and values[i] == min(segment):
                extremes.append((i, values[i]))
            elif mode == "max" and values[i] == max(segment):
                extremes.append((i, values[i]))
        return extremes
    
    def print_status(self):
        """打印 Delta/CVD 状态"""
        print(f"\n📊 Delta/CVD 状态:")
        print(f"  当前周期: 买={self.current_buy:.4f} 卖={self.current_sell:.4f} Delta={self.current_delta:+.4f}")
        print(f"  CVD: {self.cvd:+.4f} {'🟢 买方累积主导' if self.cvd > 0 else '🔴 卖方累积主导'}")
        
        div = self.get_divergence()
        if div == "bullish_div":
            print(f"  ⚠️ 看涨背离: 价格创新低但 CVD 未跟 → 潜在反弹")
        elif div == "bearish_div":
            print(f"  ⚠️ 看跌背离: 价格创新高但 CVD 未跟 → 潜在回落")

# ==================== 3. 成交量分布 (Volume Profile) ====================

class VolumeProfile:
    """
    成交量分布：显示每个价格水平的总成交量
    
    关键指标：
    - POC (Point of Control): 成交量最大的价格
    - VAH (Value Area High): 价值区上沿
    - VAL (Value Area Low): 价值区下沿
    - HVN (High Volume Node): 高成交量节点（支撑/阻力）
    - LVN (Low Volume Node): 低成交量节点（价格快速通过）
    """
    
    def __init__(self, tick_size=0.1):
        self.tick_size = tick_size
        self.profile = defaultdict(float)  # {price: volume}
        self.buy_profile = defaultdict(float)
        self.sell_profile = defaultdict(float)
    
    def _round_price(self, price):
        return round(round(price / self.tick_size) * self.tick_size, 2)
    
    def add_trades(self, trades):
        """添加成交数据"""
        for t in trades:
            price = self._round_price(float(t["p"]))
            qty = float(t["q"])
            self.profile[price] += qty
            
            if t["m"]:
                self.sell_profile[price] += qty
            else:
                self.buy_profile[price] += qty
    
    def get_poc(self):
        """获取 POC (成交量最大的价格)"""
        if not self.profile:
            return None
        return max(self.profile, key=self.profile.get)
    
    def get_value_area(self, percentage=0.70):
        """
        计算价值区 (Value Area)
        
        从 POC 开始，向上下扩展，直到包含指定百分比的总成交量
        默认 70%（行业标准）
        
        Returns:
            (vah, val, poc)
        """
        if not self.profile:
            return None, None, None
        
        poc = self.get_poc()
        total_vol = sum(self.profile.values())
        target_vol = total_vol * percentage
        
        # 从 POC 开始累积
        poc_vol = self.profile[poc]
        accumulated = poc_vol
        
        prices = sorted(self.profile.keys())
        poc_idx = prices.index(poc)
        
        up_idx = poc_idx + 1
        down_idx = poc_idx - 1
        vah = poc
        val = poc
        
        while accumulated < target_vol:
            up_vol = self.profile.get(prices[up_idx], 0) if up_idx < len(prices) else 0
            down_vol = self.profile.get(prices[down_idx], 0) if down_idx >= 0 else 0
            
            if up_vol == 0 and down_vol == 0:
                break
            
            if up_vol >= down_vol and up_idx < len(prices):
                accumulated += up_vol
                vah = prices[up_idx]
                up_idx += 1
            else:
                accumulated += down_vol
                val = prices[down_idx]
                down_idx -= 1
        
        return vah, val, poc
    
    def get_hvn_lvn(self, threshold_percentile=75):
        """
        识别 HVN 和 LVN
        
        HVN: 成交量 > 第 threshold 百分位
        LVN: 成交量 < 第 (100-threshold) 百分位
        
        Returns:
            (hvns, lvns): 两个价格列表
        """
        if not self.profile:
            return [], []
        
        vols = list(self.profile.values())
        sorted_vols = sorted(vols)
        
        high_threshold = sorted_vols[int(len(sorted_vols) * threshold_percentile / 100)]
        low_threshold = sorted_vols[int(len(sorted_vols) * (100 - threshold_percentile) / 100)]
        
        hvns = [p for p, v in self.profile.items() if v >= high_threshold]
        lvns = [p for p, v in self.profile.items() if v <= low_threshold]
        
        return sorted(hvns), sorted(lvns)
    
    def print_profile(self, top_n=20):
        """打印成交量分布"""
        if not self.profile:
            print("  无数据")
            return
        
        vah, val, poc = self.get_value_area()
        hvns, lvns = self.get_hvn_lvn()
        
        # 按成交量排序取 top N
        sorted_by_vol = sorted(self.profile.items(), key=lambda x: x[1], reverse=True)[:top_n]
        sorted_by_price = sorted(sorted_by_vol, key=lambda x: x[0], reverse=True)
        
        max_vol = max(v for _, v in sorted_by_vol)
        
        print(f"\n{'='*60}")
        print(f"  成交量分布 (Volume Profile)")
        print(f"{'='*60}")
        print(f"  POC: {poc:.1f}  VAH: {vah:.1f}  VAL: {val:.1f}  价值区宽度: {vah-val:.1f}")
        print(f"  HVN: {', '.join(f'{p:.1f}' for p in hvns[:5])}")
        print(f"  LVN: {', '.join(f'{p:.1f}' for p in lvns[:5])}")
        print()
        print(f"  {'价格':>10}  {'总量':>12}  {'买':>10}  {'卖':>10}  {'柱状图'}")
        print(f"  {'-'*10}  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*20}")
        
        for price, vol in sorted_by_price:
            buy = self.buy_profile.get(price, 0)
            sell = self.sell_profile.get(price, 0)
            bar = "█" * max(1, int(vol / max_vol * 20))
            
            marker = ""
            if price == poc:
                marker = " ◀ POC"
            elif price == vah:
                marker = " ◀ VAH"
            elif price == val:
                marker = " ◀ VAL"
            elif price in hvns:
                marker = " ● HVN"
            elif price in lvns:
                marker = " ○ LVN"
            
            print(f"  {price:>10.1f}  {vol:>12.4f}  {buy:>10.4f}  {sell:>10.4f}  {bar}{marker}")

# ==================== 4. 堆叠失衡 (Stacked Imbalance) ====================

class ImbalanceDetector:
    """
    堆叠失衡检测（支持跨 K 线）

    原理：
    - 在足迹图中，如果某价格的买量是下方价格卖量的 N 倍以上 → 买方失衡
    - 连续 3+ 个价格出现同方向失衡 → 堆叠失衡（Stacked Imbalance）
    - 跨 K 线：同一价格区域连续多根 K 线出现同方向失衡 → 更强信号

    参数：
    - ratio: 失衡比率阈值（默认 3.0，即 3:1）
    - min_stack: 最少堆叠层数（默认 3）
    """

    def __init__(self, ratio=3.0, min_stack=3):
        self.ratio = ratio
        self.min_stack = min_stack
        # 跨 K 线失衡追踪
        self.bar_imbalance_history = deque(maxlen=10)  # 最近 10 根 K 线的失衡记录
    
    def detect(self, bar_data):
        """
        检测单根K线内的堆叠失衡
        
        Args:
            bar_data: {price: {buy, sell}} 格式的足迹数据
        
        Returns:
            list of {type, price_start, price_end, strength}
        """
        if not bar_data or len(bar_data) < self.min_stack:
            return []
        
        prices = sorted(bar_data.keys())
        signals = []
        
        # 检测买方失衡
        buy_imbalance_levels = []
        for i in range(len(prices) - 1):
            upper = prices[i]     # 较高价格
            lower = prices[i + 1] # 较低价格
            
            upper_buy = bar_data[upper]["buy"]
            lower_sell = bar_data[lower]["sell"]
            
            # 买方失衡: 高价买量 / 低价卖量 > ratio
            if lower_sell > 0 and upper_buy / lower_sell >= self.ratio:
                buy_imbalance_levels.append(upper)
            elif upper_buy > 0 and lower_sell == 0:
                buy_imbalance_levels.append(upper)
        
        # 检测卖方失衡
        sell_imbalance_levels = []
        for i in range(len(prices) - 1):
            upper = prices[i]
            lower = prices[i + 1]
            
            upper_sell = bar_data[upper]["sell"]
            lower_buy = bar_data[lower]["buy"]
            
            # 卖方失衡: 低价卖量 / 高价买量 > ratio
            if lower_buy > 0 and upper_sell / lower_buy >= self.ratio:
                sell_imbalance_levels.append(lower)
            elif upper_sell > 0 and lower_buy == 0:
                sell_imbalance_levels.append(lower)
        
        # 检测堆叠（连续价格层）
        # 买方堆叠
        buy_stacks = self._find_stacks(buy_imbalance_levels, prices)
        for stack in buy_stacks:
            if len(stack) >= self.min_stack:
                signals.append({
                    "type": "stacked_buy_imbalance",
                    "direction": "bullish",
                    "bias": "bullish",
                    "price_start": stack[0],
                    "price_end": stack[-1],
                    "levels": len(stack),
                    "strength": len(stack) / self.min_stack,
                })
        
        # 卖方堆叠
        sell_stacks = self._find_stacks(sell_imbalance_levels, prices)
        for stack in sell_stacks:
            if len(stack) >= self.min_stack:
                signals.append({
                    "type": "stacked_sell_imbalance",
                    "direction": "bearish",
                    "bias": "bearish",
                    "price_start": stack[0],
                    "price_end": stack[-1],
                    "levels": len(stack),
                    "strength": len(stack) / self.min_stack,
                })
        
        return signals

    def detect_cross_bar(self, bar_data, tick_size=10.0):
        """
        跨 K 线堆叠失衡检测

        追踪最近 N 根 K 线中，同一价格区域反复出现同方向失衡的情况。
        比单根 K 线内的堆叠更可靠。

        Args:
            bar_data: 当前 K 线的足迹数据
            tick_size: 价格精度

        Returns:
            list of cross-bar imbalance signals
        """
        if not bar_data:
            return []

        # 记录当前 K 线的失衡区域
        prices = sorted(bar_data.keys())
        buy_imbalance_prices = set()
        sell_imbalance_prices = set()

        for i in range(len(prices) - 1):
            upper = prices[i]
            lower = prices[i + 1]
            upper_buy = bar_data[upper]["buy"]
            lower_sell = bar_data[lower]["sell"]
            upper_sell = bar_data[upper]["sell"]
            lower_buy = bar_data[lower]["buy"]

            if lower_sell > 0 and upper_buy / lower_sell >= self.ratio:
                buy_imbalance_prices.add(upper)
            elif upper_buy > 0 and lower_sell == 0:
                buy_imbalance_prices.add(upper)

            if lower_buy > 0 and upper_sell / lower_buy >= self.ratio:
                sell_imbalance_prices.add(lower)
            elif upper_sell > 0 and lower_buy == 0:
                sell_imbalance_prices.add(lower)

        # 保存到历史
        self.bar_imbalance_history.append({
            "buy_prices": buy_imbalance_prices,
            "sell_prices": sell_imbalance_prices,
        })

        if len(self.bar_imbalance_history) < 3:
            return []

        signals = []

        # 检查最近 3 根 K 线中，是否有同一价格区域反复出现买方失衡
        recent_buys = [h["buy_prices"] for h in self.bar_imbalance_history]
        common_buy_prices = recent_buys[0]
        for p_set in recent_buys[1:]:
            common_buy_prices = common_buy_prices & p_set

        if len(common_buy_prices) >= 2:
            buy_list = sorted(common_buy_prices)
            signals.append({
                "type": "cross_bar_buy_imbalance",
                "direction": "bullish",
                "bias": "bullish",
                "price_start": buy_list[0],
                "price_end": buy_list[-1],
                "bar_count": len(self.bar_imbalance_history),
                "levels": len(common_buy_prices),
                "strength": len(common_buy_prices) / self.min_stack,
            })

        # 检查卖方跨 K 线失衡
        recent_sells = [h["sell_prices"] for h in self.bar_imbalance_history]
        common_sell_prices = recent_sells[0]
        for p_set in recent_sells[1:]:
            common_sell_prices = common_sell_prices & p_set

        if len(common_sell_prices) >= 2:
            sell_list = sorted(common_sell_prices)
            signals.append({
                "type": "cross_bar_sell_imbalance",
                "direction": "bearish",
                "bias": "bearish",
                "price_start": sell_list[0],
                "price_end": sell_list[-1],
                "bar_count": len(self.bar_imbalance_history),
                "levels": len(common_sell_prices),
                "strength": len(common_sell_prices) / self.min_stack,
            })

        return signals

    def _find_stacks(self, imbalance_levels, all_prices):
        """找出连续的价格层"""
        if not imbalance_levels:
            return []

        imbalance_set = set(imbalance_levels)
        stacks = []
        current_stack = []

        for price in all_prices:
            if price in imbalance_set:
                current_stack.append(price)
            else:
                if len(current_stack) >= self.min_stack:
                    stacks.append(current_stack)
                current_stack = []

        if len(current_stack) >= self.min_stack:
            stacks.append(current_stack)

        return stacks

# ==================== 5. 吸收检测 (Absorption) ====================

class AbsorptionDetector:
    """
    吸收检测（按市场状态建基线）

    定义：大量主动成交发生，但价格几乎不动
    → 说明有大量被动限价单在吸收主动单

    检测公式（三个条件同时满足）：
    1. Volume Z-Score > threshold（成交量统计显著异常）
    2. Net Taker Imbalance > 60% 或 < -60%（极端方向性）
    3. Relative Price Impact < 0.8（价格几乎没动）

    改进：按成交量高低分别建基线，避免高波动时段误判
    """

    def __init__(self, z_threshold=3.0, imbalance_threshold=0.6, price_impact_threshold=0.8):
        self.z_threshold = z_threshold
        self.imbalance_threshold = imbalance_threshold
        self.price_impact_threshold = price_impact_threshold
        self.volume_history = deque(maxlen=100)
        self.price_impact_history = deque(maxlen=50)
    
    def detect(self, trades, window_seconds=60):
        """
        检测吸收事件
        
        Args:
            trades: aggTrades 列表
            window_seconds: 检测窗口（秒）
        
        Returns:
            list of absorption events
        """
        if not trades:
            return []
        
        # 按时间窗口分组
        windows = defaultdict(list)
        for t in trades:
            window_key = int(t["T"] / 1000 / window_seconds) * window_seconds
            windows[window_key].append(t)
        
        signals = []
        
        for window_time, window_trades in windows.items():
            if len(window_trades) < 10:
                continue
            
            # 计算指标
            total_vol = sum(float(t["q"]) for t in window_trades)
            buy_vol = sum(float(t["q"]) for t in window_trades if not t["m"])
            sell_vol = sum(float(t["q"]) for t in window_trades if t["m"])
            
            # 1. Volume Z-Score
            self.volume_history.append(total_vol)
            if len(self.volume_history) < 10:
                continue
            
            mean_vol = sum(self.volume_history) / len(self.volume_history)
            std_vol = math.sqrt(sum((v - mean_vol) ** 2 for v in self.volume_history) / len(self.volume_history))
            
            if std_vol == 0:
                continue
            
            z_score = (total_vol - mean_vol) / std_vol
            
            # 2. Net Taker Imbalance
            net_imbalance = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0
            
            # 3. Relative Price Impact
            prices = [float(t["p"]) for t in window_trades]
            price_range = max(prices) - min(prices)
            mid_price = (max(prices) + min(prices)) / 2
            relative_impact = price_range / mid_price * 100 if mid_price > 0 else 1
            
            # 归一化价格影响（相对于历史波动）
            self.price_impact_history.append(relative_impact)
            if len(self.price_impact_history) >= 5:
                avg_impact = sum(self.price_impact_history) / len(self.price_impact_history)
                normal_impact = relative_impact / avg_impact if avg_impact > 0 else 1
            else:
                normal_impact = 1  # 数据不足，默认不算吸收
            
            # 检测吸收
            if z_score > self.z_threshold:
                if abs(net_imbalance) > self.imbalance_threshold:
                    if normal_impact < self.price_impact_threshold:
                        direction = "buyer_absorption" if net_imbalance > 0 else "seller_absorption"
                        bias = "bullish" if direction == "seller_absorption" else "bearish"
                        
                        signals.append({
                            "time": window_time,
                            "type": "absorption",
                            "direction": direction,
                            "bias": bias,
                            "z_score": z_score,
                            "net_imbalance": net_imbalance,
                            "price_impact": normal_impact,
                            "volume": total_vol,
                            "buy_vol": buy_vol,
                            "sell_vol": sell_vol,
                            "price": mid_price,
                        })
        
        return signals

# ==================== 6. 衰竭检测 (Exhaustion) ====================

class ExhaustionDetector:
    """
    衰竭检测

    定义：价格到达极端位置后，成交量逐渐萎缩
    → 说明推动力量耗尽，反转在即

    检测方法：
    1. 价格处于近期高/低点
    2. 最近 N 个周期成交量递减（阈值从 0.5 降到 0.3，更灵敏）
    3. Delta 方向与价格方向不一致，且 Delta 绝对值有意义
    """

    def __init__(self, lookback=10, volume_decline_threshold=0.3):
        self.lookback = lookback
        self.volume_decline_threshold = volume_decline_threshold

    def detect(self, delta_history):
        """
        检测衰竭

        Args:
            delta_history: DeltaTracker.history 列表

        Returns:
            list of exhaustion events
        """
        if len(delta_history) < self.lookback:
            return []

        recent = delta_history[-self.lookback:]
        signals = []

        # 检查成交量递减
        vols = [h["buy"] + h["sell"] for h in recent]
        prices = [h["price"] for h in recent]

        if len(vols) >= 3:
            recent_vol_avg = sum(vols[-3:]) / 3
            earlier_vol_avg = sum(vols[:-3]) / max(1, len(vols) - 3)

            vol_decline = 1 - (recent_vol_avg / earlier_vol_avg) if earlier_vol_avg > 0 else 0

            if vol_decline > self.volume_decline_threshold:
                current_price = prices[-1]
                price_high = max(prices)
                price_low = min(prices)
                price_range = price_high - price_low

                if price_range > 0:
                    # 高点衰竭（看跌）：价格在高位 + Delta 转负且有意义
                    if (price_high - current_price) / price_range < 0.2:
                        recent_delta = sum(h["delta"] for h in recent[-3:])
                        avg_vol = sum(vols[-3:]) / 3
                        # Delta 绝对值要超过平均成交量的 10% 才有意义
                        if recent_delta < 0 and avg_vol > 0 and abs(recent_delta) / avg_vol > 0.1:
                            signals.append({
                                "type": "exhaustion",
                                "direction": "bearish_exhaustion",
                                "bias": "bearish",
                                "vol_decline": vol_decline,
                                "price": current_price,
                                "recent_delta": recent_delta,
                            })

                    # 低点衰竭（看涨）：价格在低位 + Delta 转正且有意义
                    if (current_price - price_low) / price_range < 0.2:
                        recent_delta = sum(h["delta"] for h in recent[-3:])
                        avg_vol = sum(vols[-3:]) / 3
                        if recent_delta > 0 and avg_vol > 0 and abs(recent_delta) / avg_vol > 0.1:
                            signals.append({
                                "type": "exhaustion",
                                "direction": "bullish_exhaustion",
                                "bias": "bullish",
                                "vol_decline": vol_decline,
                                "price": current_price,
                                "recent_delta": recent_delta,
                            })

        return signals

# ==================== 7. 冰山单检测 (Iceberg Detection) ====================

class IcebergDetector:
    """
    冰山单检测
    
    原理：
    - 冰山单 = 大单拆成小单，在同一价格反复出现
    - 特征：同一价格水平出现异常多次成交，每次成交量相近
    
    检测方法：
    1. 统计每个价格的成交次数和总成交量
    2. 如果某价格成交次数远超平均 → 疑似冰山
    3. 如果单次成交量标准差小 → 更确定是冰山（算法拆单）
    """
    
    def __init__(self, min_fills=10, max_cv=0.5, z_threshold=2.0):
        self.min_fills = min_fills      # 最少成交笔数
        self.max_cv = max_cv            # 变异系数上限（越小说明单量越均匀）
        self.z_threshold = z_threshold  # 成交次数 Z-Score 阈值
    
    def detect(self, trades):
        """
        检测冰山单
        
        Args:
            trades: aggTrades 列表
        
        Returns:
            list of detected iceberg orders
        """
        # 按价格聚合
        price_fills = defaultdict(list)  # {price: [qty1, qty2, ...]}
        
        for t in trades:
            price = float(t["p"])
            qty = float(t["q"])
            price_fills[price].append(qty)
        
        if not price_fills:
            return []
        
        # 统计成交次数
        fill_counts = {p: len(qtys) for p, qtys in price_fills.items()}
        all_counts = list(fill_counts.values())
        mean_count = sum(all_counts) / len(all_counts)
        std_count = math.sqrt(sum((c - mean_count) ** 2 for c in all_counts) / len(all_counts))
        
        signals = []
        
        for price, qtys in price_fills.items():
            n_fills = len(qtys)
            
            # 条件1: 成交次数足够多
            if n_fills < self.min_fills:
                continue
            
            # 条件2: 成交次数统计异常
            if std_count > 0:
                z_score = (n_fills - mean_count) / std_count
                if z_score < self.z_threshold:
                    continue
            else:
                continue
            
            # 条件3: 单次成交量相对均匀（变异系数小）
            mean_qty = sum(qtys) / len(qtys)
            if mean_qty > 0:
                std_qty = math.sqrt(sum((q - mean_qty) ** 2 for q in qtys) / len(qtys))
                cv = std_qty / mean_qty
                
                if cv <= self.max_cv:
                    total_vol = sum(qtys)
                    # 按该价格的实际成交统计方向
                    price_trades = [t for t in trades if float(t["p"]) == price]
                    buy_count = sum(1 for t in price_trades if not t["m"])
                    sell_count = sum(1 for t in price_trades if t["m"])
                    direction = "buy" if buy_count > sell_count else "sell" if sell_count > buy_count else "unknown"
                    
                    signals.append({
                        "type": "iceberg",
                        "price": price,
                        "fills": n_fills,
                        "avg_qty": mean_qty,
                        "total_volume": total_vol,
                        "cv": cv,
                        "z_score": z_score,
                        "direction": direction,  # 从该价格实际成交统计
                    })
        
        return signals

# ==================== 8. 成交速度 (Speed of Tape) ====================

class SpeedOfTape:
    """
    成交速度 / 动量指标
    
    原理：
    - 速度 = 单位时间内的成交笔数
    - 加速度 = 速度的变化率
    - 速度快 = 市场活跃，方向性可能延续
    - 速度减速 = 推动力量减弱，可能反转
    
    用途：
    - 突破确认：突破时速度加速 → 真突破
    - 衰竭识别：趋势中速度递减 → 假突破/反转
    """
    
    def __init__(self, window_seconds=10, history_size=60):
        self.window_seconds = window_seconds
        self.speed_history = deque(maxlen=history_size)  # [(timestamp, speed, buy_speed, sell_speed)]
    
    def add_trades(self, trades):
        """添加成交数据，计算速度"""
        # 按窗口分组
        windows = defaultdict(list)
        for t in trades:
            window_key = int(t["T"] / 1000 / self.window_seconds) * self.window_seconds
            windows[window_key].append(t)
        
        for window_time, window_trades in sorted(windows.items()):
            speed = len(window_trades) / self.window_seconds
            buy_speed = sum(1 for t in window_trades if not t["m"]) / self.window_seconds
            sell_speed = sum(1 for t in window_trades if t["m"]) / self.window_seconds
            buy_vol_speed = sum(float(t["q"]) for t in window_trades if not t["m"]) / self.window_seconds
            sell_vol_speed = sum(float(t["q"]) for t in window_trades if t["m"]) / self.window_seconds
            
            self.speed_history.append({
                "time": window_time,
                "speed": speed,
                "buy_speed": buy_speed,
                "sell_speed": sell_speed,
                "buy_vol_speed": buy_vol_speed,
                "sell_vol_speed": sell_vol_speed,
            })
    
    def get_acceleration(self, lookback=5):
        """
        计算加速度（速度变化率）
        
        Returns:
            float: 正=加速, 负=减速, 0=不变
        """
        if len(self.speed_history) < lookback:
            return 0
        
        recent = list(self.speed_history)[-lookback:]
        speeds = [s["speed"] for s in recent]
        
        # 简单线性回归斜率
        n = len(speeds)
        x_mean = (n - 1) / 2
        y_mean = sum(speeds) / n
        
        numerator = sum((i - x_mean) * (speeds[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        
        if denominator == 0:
            return 0
        
        slope = numerator / denominator
        return slope
    
    def get_momentum(self):
        """
        动量信号
        
        Returns:
            str: "accelerating_buy" / "accelerating_sell" / "decelerating" / "neutral"
        """
        if len(self.speed_history) < 3:
            return "neutral"
        
        recent = list(self.speed_history)[-3:]
        
        buy_speeds = [s["buy_vol_speed"] for s in recent]
        sell_speeds = [s["sell_vol_speed"] for s in recent]
        
        buy_accel = buy_speeds[-1] - buy_speeds[0]
        sell_accel = sell_speeds[-1] - sell_speeds[0]
        
        if buy_accel > 0 and buy_accel > sell_accel:
            return "accelerating_buy"
        elif sell_accel > 0 and sell_accel > buy_accel:
            return "accelerating_sell"
        elif buy_accel < 0 and sell_accel < 0:
            return "decelerating"
        
        return "neutral"
    
    def print_status(self):
        """打印速度状态"""
        if not self.speed_history:
            print("  无数据")
            return
        
        latest = self.speed_history[-1]
        accel = self.get_acceleration()
        momentum = self.get_momentum()
        
        momentum_icons = {
            "accelerating_buy": "🟢 买方加速",
            "accelerating_sell": "🔴 卖方加速",
            "decelerating": "⚠️ 双方减速",
            "neutral": "➡️ 中性",
        }
        
        print(f"\n📊 成交速度 (Speed of Tape):")
        print(f"  当前速度: {latest['speed']:.1f} 笔/秒 (买:{latest['buy_speed']:.1f} 卖:{latest['sell_speed']:.1f})")
        print(f"  成交量速度: 买:{latest['buy_vol_speed']:.4f}/s 卖:{latest['sell_vol_speed']:.4f}/s")
        print(f"  加速度: {accel:+.3f} {'📈 加速' if accel > 0 else '📉 减速' if accel < 0 else '➡️ 稳定'}")
        print(f"  动量: {momentum_icons.get(momentum, momentum)}")

# ==================== 9. 信号聚合器 ====================

class OrderFlowSignalEngine:
    """
    订单流信号聚合引擎
    
    综合所有指标，生成交易信号
    """
    
    def __init__(self, tick_size=0.1):
        self.footprint = FootprintChart(tick_size)
        self.delta = DeltaTracker()
        self.volume_profile = VolumeProfile(tick_size)
        self.imbalance = ImbalanceDetector(ratio=3.0, min_stack=3)
        self.absorption = AbsorptionDetector()
        self.exhaustion = ExhaustionDetector()
        self.iceberg = IcebergDetector()
        self.speed = SpeedOfTape()
        
        self.signals = []
    
    def feed(self, trades):
        """喂入成交数据，更新所有指标"""
        self.footprint.add_trades(trades)
        self.delta.add_trades(trades)
        self.volume_profile.add_trades(trades)
        self.speed.add_trades(trades)
    
    def analyze(self, trades):
        """运行全部分析，返回信号"""
        all_signals = []

        # 1. 堆叠失衡（单 K 线）
        _, latest_bar = self.footprint.get_latest_bar()
        if latest_bar:
            imb_signals = self.imbalance.detect(latest_bar)
            for s in imb_signals:
                s["source"] = "stacked_imbalance"
                all_signals.append(s)

            # 1b. 跨 K 线堆叠失衡
            cross_signals = self.imbalance.detect_cross_bar(latest_bar, self.footprint.tick_size)
            for s in cross_signals:
                s["source"] = "cross_bar_imbalance"
                all_signals.append(s)

        # 2. 吸收
        abs_signals = self.absorption.detect(trades)
        for s in abs_signals:
            s["source"] = "absorption"
            all_signals.append(s)

        # 3. 衰竭
        exh_signals = self.exhaustion.detect(self.delta.history)
        for s in exh_signals:
            s["source"] = "exhaustion"
            all_signals.append(s)

        # 4. 冰山单
        ice_signals = self.iceberg.detect(trades)
        for s in ice_signals:
            s["source"] = "iceberg"
            all_signals.append(s)

        # 5. CVD 背离（结构性）
        div = self.delta.get_divergence()
        if div != "none":
            all_signals.append({
                "source": "cvd_divergence",
                "type": "divergence",
                "direction": div,
                "bias": "bullish" if div == "bullish_div" else "bearish",
            })

        # 6. 速度动量
        momentum = self.speed.get_momentum()
        if momentum in ("accelerating_buy", "accelerating_sell"):
            all_signals.append({
                "source": "speed_of_tape",
                "type": "momentum",
                "direction": momentum,
                "bias": "bullish" if momentum == "accelerating_buy" else "bearish",
            })

        self.signals = all_signals
        return all_signals
    
    def get_consensus(self):
        """
        信号共识
        
        综合所有信号的偏向，给出最终建议
        """
        if not self.signals:
            return "neutral", 0
        
        bullish_count = sum(1 for s in self.signals if s.get("bias") == "bullish")
        bearish_count = sum(1 for s in self.signals if s.get("bias") == "bearish")
        total = len(self.signals)
        
        # 加权评分（吸收和堆叠失衡权重更高，跨 K 线失衡权重最高）
        weights = {
            "absorption": 3.0,
            "cross_bar_buy_imbalance": 3.5,
            "cross_bar_sell_imbalance": 3.5,
            "stacked_buy_imbalance": 2.5,
            "stacked_sell_imbalance": 2.5,
            "exhaustion": 2.0,
            "cvd_divergence": 1.5,
            "speed_of_tape": 1.0,
            "iceberg": 1.0,
        }
        
        bullish_score = sum(weights.get(s.get("source", ""), 1.0) for s in self.signals if s.get("bias") == "bullish")
        bearish_score = sum(weights.get(s.get("source", ""), 1.0) for s in self.signals if s.get("bias") == "bearish")
        
        total_score = bullish_score + bearish_score
        if total_score == 0:
            return "neutral", 0
        
        confidence = abs(bullish_score - bearish_score) / total_score
        
        if bullish_score > bearish_score * 1.5:
            return "bullish", confidence
        elif bearish_score > bullish_score * 1.5:
            return "bearish", confidence
        else:
            return "neutral", confidence
    
    def print_full_report(self):
        """打印完整分析报告"""
        # 获取当前价格
        poc = self.volume_profile.get_poc()
        vah, val, _ = self.volume_profile.get_value_area()
        
        print(f"\n{'='*60}")
        print(f"  订单流分析报告 (Order Flow Analysis)")
        print(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        # Delta/CVD
        self.delta.print_status()
        
        # 速度
        self.speed.print_status()
        
        # 成交量分布
        self.volume_profile.print_profile(top_n=10)
        
        # 信号汇总
        consensus, confidence = self.get_consensus()
        
        print(f"\n{'='*60}")
        print(f"  信号汇总")
        print(f"{'='*60}")
        
        if self.signals:
            for s in self.signals:
                icon = "🟢" if s.get("bias") == "bullish" else "🔴" if s.get("bias") == "bearish" else "⚪"
                print(f"  {icon} [{s.get('source', '?')}] {s.get('type', '?')} → {s.get('bias', '?')}")
                if "price_start" in s:
                    print(f"     价格区间: {s['price_start']:.1f} - {s['price_end']:.1f}")
                if "z_score" in s:
                    print(f"     Z-Score: {s['z_score']:.2f}  失衡: {s.get('net_imbalance', 0):.2%}")
        else:
            print("  无明确信号")
        
        # 共识
        consensus_icons = {"bullish": "🟢 做多", "bearish": "🔴 做空", "neutral": "⚪ 观望"}
        print(f"\n  📊 综合判定: {consensus_icons.get(consensus, consensus)} (置信度: {confidence:.0%})")
        
        # 关键价位
        if poc and vah and val:
            print(f"\n  📍 关键价位:")
            print(f"     POC: {poc:.1f} (磁力位)")
            print(f"     VAH: {vah:.1f} (价值区上沿)")
            print(f"     VAL: {val:.1f} (价值区下沿)")
        
        return consensus, confidence


# ==================== 10. 市场状态识别 (Market Regime Detection) ====================

class MarketRegimeDetector:
    """
    市场状态识别

    判断当前市场处于哪种状态，不同状态使用不同策略：
    - trending_up: 上升趋势，跟随做多信号
    - trending_down: 下降趋势，跟随做空信号
    - ranging: 震荡，在 VAH/VAL 做均值回归
    - breakout: 突破，等待量能确认
    - low_volatility: 低波动，不交易

    检测方法:
    1. ATR 波动率 vs 历史均值
    2. 价格与 VA 的关系
    3. CVD 趋势方向
    4. Delta 一致性（连续 N 个周期同方向）
    """

    REGIME_TRENDING_UP = "trending_up"
    REGIME_TRENDING_DOWN = "trending_down"
    REGIME_RANGING = "ranging"
    REGIME_BREAKOUT = "breakout"
    REGIME_LOW_VOL = "low_volatility"

    def __init__(self):
        self.atr_history = deque(maxlen=50)
        self.regime_history = deque(maxlen=20)

    def detect(self, delta_history, volume_profile, current_price, trades):
        """
        检测当前市场状态

        Args:
            delta_history: DeltaTracker.history
            volume_profile: VolumeProfile 对象
            current_price: 当前价格
            trades: 最近的 aggTrades

        Returns:
            dict: {regime, confidence, atr_ratio, delta_consistency, details}
        """
        result = {
            "regime": self.REGIME_RANGING,
            "confidence": 0.5,
            "atr_ratio": 1.0,
            "delta_consistency": 0,
            "details": "",
        }

        # 1. ATR 波动率分析
        atr, atr_ratio = self._calc_atr_ratio(delta_history)
        result["atr_ratio"] = atr_ratio

        # 2. 价格与 VA 关系
        vah, val, poc = volume_profile.get_value_area()
        if not poc:
            return result

        price_vs_poc = (current_price - poc) / poc if poc else 0
        va_width = (vah - val) / poc if poc and vah and val else 0

        # 3. Delta 一致性（最近 5 个周期）
        consistency = self._calc_delta_consistency(delta_history)
        result["delta_consistency"] = consistency

        # 4. 综合判断
        # 低波动
        if atr_ratio < 0.5:
            result["regime"] = self.REGIME_LOW_VOL
            result["confidence"] = 0.7
            result["details"] = f"ATR 仅为均值 {atr_ratio:.0%}，波动率过低"
            self.regime_history.append(result["regime"])
            return result

        # 突破：价格离开 VA + 高波动 + Delta 一致
        if va_width > 0 and current_price > vah and atr_ratio > 1.2 and consistency > 0.6:
            result["regime"] = self.REGIME_BREAKOUT
            result["confidence"] = min(0.9, 0.6 + consistency * 0.3)
            result["details"] = f"价格突破 VAH({vah:.0f})，ATR={atr_ratio:.1f}x，Delta一致性={consistency:.0%}"
            self.regime_history.append(result["regime"])
            return result

        if va_width > 0 and current_price < val and atr_ratio > 1.2 and consistency < -0.6:
            result["regime"] = self.REGIME_BREAKOUT
            result["confidence"] = min(0.9, 0.6 + abs(consistency) * 0.3)
            result["details"] = f"价格跌破 VAL({val:.0f})，ATR={atr_ratio:.1f}x，Delta一致性={consistency:.0%}"
            self.regime_history.append(result["regime"])
            return result

        # 趋势：Delta 一致 + 价格偏向一侧
        if consistency > 0.6 and price_vs_poc > 0.001:
            result["regime"] = self.REGIME_TRENDING_UP
            result["confidence"] = min(0.85, 0.5 + consistency * 0.3)
            result["details"] = f"CVD 连续看多，价格在 POC 上方 {price_vs_poc:.2%}"
            self.regime_history.append(result["regime"])
            return result

        if consistency < -0.6 and price_vs_poc < -0.001:
            result["regime"] = self.REGIME_TRENDING_DOWN
            result["confidence"] = min(0.85, 0.5 + abs(consistency) * 0.3)
            result["details"] = f"CVD 连续看空，价格在 POC 下方 {abs(price_vs_poc):.2%}"
            self.regime_history.append(result["regime"])
            return result

        # 震荡（默认）
        result["regime"] = self.REGIME_RANGING
        result["confidence"] = 0.5
        result["details"] = f"价格在 VA 内，无明确方向，ATR={atr_ratio:.1f}x"
        self.regime_history.append(result["regime"])
        return result

    def _calc_atr_ratio(self, delta_history):
        """计算当前 ATR 相对于历史均值的比率"""
        if len(delta_history) < 10:
            return 0, 1.0

        # 用 delta_history 的价格计算简易 ATR（真实波幅）
        trs = []
        for i in range(1, len(delta_history)):
            high = max(delta_history[i]["price"], delta_history[i-1]["price"])
            low = min(delta_history[i]["price"], delta_history[i-1]["price"])
            trs.append(high - low)

        if len(trs) < 5:
            return 0, 1.0

        current_atr = sum(trs[-5:]) / 5
        historical_atr = sum(trs) / len(trs)

        self.atr_history.append(current_atr)

        atr_ratio = current_atr / historical_atr if historical_atr > 0 else 1.0
        return current_atr, atr_ratio

    def _calc_delta_consistency(self, delta_history):
        """
        计算最近 N 个周期 Delta 方向一致性

        Returns:
            float: -1 到 1，正=多头一致，负=空头一致，0=无一致
        """
        if len(delta_history) < 5:
            return 0

        recent = delta_history[-5:]
        bullish = sum(1 for h in recent if h["delta"] > 0)
        bearish = sum(1 for h in recent if h["delta"] < 0)

        return (bullish - bearish) / len(recent)


# ==================== 11. ATR 计算器 ====================

class ATRCalculator:
    """
    ATR (Average True Range) 波动率计算

    用于:
    - 动态止损距离
    - 自适应信号阈值
    - 仓位大小调整
    """

    def __init__(self, period=14):
        self.period = period
        self.tr_history = deque(maxlen=200)

    def update(self, trades, bar_seconds=300):
        """从成交数据计算 ATR"""
        if len(trades) < 2:
            return

        # 按 K 线分组
        bars = defaultdict(list)
        for t in trades:
            bar_key = int(t["T"] / 1000 / bar_seconds) * bar_seconds
            bars[bar_key].append(float(t["p"]))

        if len(bars) < 2:
            return

        sorted_bars = sorted(bars.items())
        for i in range(1, len(sorted_bars)):
            prev_prices = sorted_bars[i-1][1]
            curr_prices = sorted_bars[i][1]

            high = max(curr_prices)
            low = min(curr_prices)
            prev_close = prev_prices[-1]

            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            self.tr_history.append(tr)

    def get_atr(self):
        """获取当前 ATR"""
        if len(self.tr_history) < self.period:
            return None
        recent = list(self.tr_history)[-self.period:]
        return sum(recent) / len(recent)

    def get_atr_pct(self, current_price):
        """获取 ATR 占价格的百分比"""
        atr = self.get_atr()
        if atr and current_price > 0:
            return atr / current_price
        return 0.005  # 默认 0.5%


# ==================== 12. 订单簿失衡 (Book Imbalance) ====================

class BookImbalance:
    """
    订单簿失衡检测

    原理:
    - DOM (Depth of Market) 的 bid/ask 比率反映买卖力量对比
    - bid_vol / ask_vol > 1.5 → 买方支撑强 → 看涨
    - bid_vol / ask_vol < 0.67 → 卖方压力大 → 看跌

    用途:
    - 作为订单流信号的确认指标
    - 与吸收/失衡信号结合使用
    """

    def __init__(self, bullish_threshold=1.5, bearish_threshold=0.67):
        self.bullish_threshold = bullish_threshold
        self.bearish_threshold = bearish_threshold

    def analyze(self, depth_data):
        """
        分析订单簿失衡

        Args:
            depth_data: fetch_depth() 返回的数据 {"bids": [...], "asks": [...]}

        Returns:
            dict: {ratio, bias, bid_vol, ask_vol, bid_wall, ask_wall}
        """
        if not depth_data:
            return {"ratio": 1.0, "bias": "neutral", "bid_vol": 0, "ask_vol": 0,
                    "bid_wall": None, "ask_wall": None}

        bids = depth_data.get("bids", [])
        asks = depth_data.get("asks", [])

        if not bids or not asks:
            return {"ratio": 1.0, "bias": "neutral", "bid_vol": 0, "ask_vol": 0,
                    "bid_wall": None, "ask_wall": None}

        bid_vol = sum(float(b[1]) for b in bids)
        ask_vol = sum(float(a[1]) for a in asks)

        ratio = bid_vol / ask_vol if ask_vol > 0 else 2.0

        if ratio >= self.bullish_threshold:
            bias = "bullish"
        elif ratio <= self.bearish_threshold:
            bias = "bearish"
        else:
            bias = "neutral"

        # 找最大挂单墙
        bid_wall = max(bids, key=lambda x: float(x[1])) if bids else None
        ask_wall = max(asks, key=lambda x: float(x[1])) if asks else None

        return {
            "ratio": ratio,
            "bias": bias,
            "bid_vol": bid_vol,
            "ask_vol": ask_vol,
            "bid_wall": float(bid_wall[0]) if bid_wall else None,
            "bid_wall_vol": float(bid_wall[1]) if bid_wall else 0,
            "ask_wall": float(ask_wall[0]) if ask_wall else None,
            "ask_wall_vol": float(ask_wall[1]) if ask_wall else 0,
        }


# ==================== 13. 多时间框架确认 (Multi-Timeframe Confirmation) ====================

class MultiTimeframeConfirm:
    """
    多时间框架确认

    原理:
    - 对比不同时间窗口的 Volume Profile 和 Delta 方向
    - 短周期信号 + 长周期同方向确认 = 更高胜率
    - 短周期做多但长周期空头主导 → 信号可能不可靠

    实现:
    - 使用同一份 aggTrade 数据，按不同窗口大小聚合
    - 对比 1min / 5min / 15min 的 Delta 和 VP
    """

    def __init__(self):
        self.timeframes = {
            "1m": {"delta": 0, "bias": "neutral"},
            "5m": {"delta": 0, "bias": "neutral"},
            "15m": {"delta": 0, "bias": "neutral"},
        }

    def analyze(self, trades):
        """
        多时间框架分析

        Args:
            trades: aggTrades 列表

        Returns:
            dict: {agreement, bias, details, score}
        """
        if not trades or len(trades) < 100:
            return {"agreement": 0, "bias": "neutral", "details": "数据不足", "score": 0}

        # 按不同时间窗口计算 Delta
        results = {}
        for tf_name, tf_seconds in [("1m", 60), ("5m", 300), ("15m", 900)]:
            delta = self._calc_delta_by_window(trades, tf_seconds)
            bias = "bullish" if delta > 0 else "bearish" if delta < 0 else "neutral"
            results[tf_name] = {"delta": delta, "bias": bias}

        self.timeframes = results

        # 计算一致性
        biases = [v["bias"] for v in results.values()]
        bullish_count = sum(1 for b in biases if b == "bullish")
        bearish_count = sum(1 for b in biases if b == "bearish")

        if bullish_count == 3:
            return {"agreement": 1.0, "bias": "bullish",
                    "details": "1m/5m/15m 全部看多", "score": 3}
        elif bearish_count == 3:
            return {"agreement": 1.0, "bias": "bearish",
                    "details": "1m/5m/15m 全部看空", "score": -3}
        elif bullish_count == 2:
            return {"agreement": 0.67, "bias": "bullish",
                    "details": f"2/3 时间框架看多 ({', '.join(tf for tf, v in results.items() if v['bias']=='bullish')})",
                    "score": 1}
        elif bearish_count == 2:
            return {"agreement": 0.67, "bias": "bearish",
                    "details": f"2/3 时间框架看空 ({', '.join(tf for tf, v in results.items() if v['bias']=='bearish')})",
                    "score": -1}
        else:
            return {"agreement": 0, "bias": "neutral",
                    "details": "时间框架方向不一致", "score": 0}

    def _calc_delta_by_window(self, trades, window_seconds):
        """按指定窗口计算总 Delta"""
        buy_vol = 0.0
        sell_vol = 0.0

        # 只取最近 N 个窗口的数据
        now = trades[-1]["T"] / 1000
        cutoff = now - window_seconds * 5  # 最近 5 个窗口

        for t in trades:
            ts = t["T"] / 1000
            if ts < cutoff:
                continue
            qty = float(t["q"])
            if t["m"]:
                sell_vol += qty
            else:
                buy_vol += qty

        return buy_vol - sell_vol


# ==================== 14. OI + 资金费率分析 ====================

class OIFundingAnalyzer:
    """
    持仓量 (OI) + 资金费率分析

    信号逻辑:
    - OI 增 + 价格涨 → 新多头入场，趋势健康 (bullish)
    - OI 减 + 价格涨 → 空头平仓，反弹可能结束 (bearish)
    - OI 增 + 价格跌 → 新空头入场，下跌趋势 (bearish)
    - OI 减 + 价格跌 → 多头平仓，抛压减弱 (bullish)
    - 资金费率 > 0.1% → 多头过热，逆向看空
    - 资金费率 < -0.1% → 空头过热，逆向看多
    """

    def __init__(self, api_mode="futures_demo"):
        self.api_mode = api_mode
        self.funding_extreme_threshold = 0.001  # 0.1%

    def analyze(self, symbol="BTCUSDT"):
        """
        获取并分析 OI 和资金费率

        Returns:
            dict: {oi_change, funding_rate, bias, confidence, details}
        """
        result = {
            "oi_change": 0,
            "funding_rate": 0,
            "bias": "neutral",
            "confidence": 0,
            "details": "",
        }

        try:
            from config import FUTURES_DEMO_BASE
            base = FUTURES_DEMO_BASE
        except ImportError:
            return result

        # 获取资金费率
        try:
            proxies = get_proxies()
            r = requests.get(f"{base}/fapi/v1/premiumIndex",
                           params={"symbol": symbol},
                           proxies=proxies, timeout=10)
            if r.status_code == 200:
                data = r.json()
                result["funding_rate"] = float(data.get("lastFundingRate", 0))
        except Exception:
            pass

        # 获取 OI 历史
        try:
            r = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                           params={"symbol": symbol, "period": "1h", "limit": 5},
                           proxies=proxies, timeout=10)
            if r.status_code == 200 and isinstance(r.json(), list) and len(r.json()) >= 2:
                oi_data = r.json()
                oi_first = float(oi_data[0]["sumOpenInterest"])
                oi_last = float(oi_data[-1]["sumOpenInterest"])
                if oi_first > 0:
                    result["oi_change"] = (oi_last - oi_first) / oi_first
        except Exception:
            pass

        # 综合判断
        funding = result["funding_rate"]
        oi_chg = result["oi_change"]

        signals = []

        # 资金费率极端
        if funding > self.funding_extreme_threshold:
            signals.append(("bearish", 0.6, f"资金费率 {funding*100:.3f}% 过高，多头过热"))
        elif funding < -self.funding_extreme_threshold:
            signals.append(("bullish", 0.6, f"资金费率 {funding*100:.3f}% 过低，空头过热"))

        # OI 变化（需要配合价格方向，这里简化处理）
        if abs(oi_chg) > 0.02:  # OI 变化超过 2%
            if oi_chg > 0:
                signals.append(("bullish", 0.4, f"OI 增加 {oi_chg:.1%}，新资金入场"))
            else:
                signals.append(("bearish", 0.4, f"OI 减少 {abs(oi_chg):.1%}，资金撤离"))

        if signals:
            # 取最强信号
            best = max(signals, key=lambda x: x[1])
            result["bias"] = best[0]
            result["confidence"] = best[1]
            result["details"] = best[2]
        else:
            result["details"] = f"资金费率={funding*100:.3f}%, OI变化={oi_chg:.1%}"

        return result


# ==================== 15. 自适应参数 (Adaptive Parameters) ====================

class AdaptiveParams:
    """
    基于 ATR 的自适应参数

    根据市场波动率动态调整:
    - 止损距离: 高波动放宽，低波动收紧
    - 信号阈值: 高波动要求更强信号
    - 仓位大小: 高波动减仓，低波动加仓
    """

    def __init__(self, base_stop_pct=0.0065, base_tp_pct=0.013):
        self.base_stop_pct = base_stop_pct
        self.base_tp_pct = base_tp_pct

    def get_params(self, atr_pct, regime):
        """
        根据 ATR 和市场状态返回自适应参数

        Args:
            atr_pct: ATR 占价格的百分比
            regime: 市场状态字符串

        Returns:
            dict: {stop_loss_pct, take_profit_pct, min_signals, qty_multiplier}
        """
        # ATR 基础调整
        # 用 ATR 的 1.5 倍作为止损
        atr_stop = max(0.003, min(0.015, atr_pct * 1.5))
        atr_tp = atr_stop * 2  # 2:1 R:R

        # 市场状态调整
        regime_multipliers = {
            MarketRegimeDetector.REGIME_TRENDING_UP: {"stop": 1.0, "tp": 1.2, "sig": 1, "qty": 1.0},
            MarketRegimeDetector.REGIME_TRENDING_DOWN: {"stop": 1.0, "tp": 1.2, "sig": 1, "qty": 1.0},
            MarketRegimeDetector.REGIME_RANGING: {"stop": 0.8, "tp": 0.8, "sig": 2, "qty": 0.8},
            MarketRegimeDetector.REGIME_BREAKOUT: {"stop": 1.2, "tp": 1.5, "sig": 1, "qty": 1.0},
            MarketRegimeDetector.REGIME_LOW_VOL: {"stop": 0.6, "tp": 0.6, "sig": 3, "qty": 0.5},
        }

        mult = regime_multipliers.get(regime, regime_multipliers[MarketRegimeDetector.REGIME_RANGING])

        return {
            "stop_loss_pct": atr_stop * mult["stop"],
            "take_profit_pct": atr_tp * mult["tp"],
            "min_signals": max(2, mult["sig"]),
            "qty_multiplier": mult["qty"],
        }


# ==================== 16. 动态仓位管理 (Dynamic Position Sizer) ====================

class DynamicPositionSizer:
    """
    动态仓位管理

    根据以下因素动态调整仓位:
    1. 信号强度（一致信号数量）
    2. 市场状态（趋势/震荡/突破）
    3. ATR 波动率
    4. 账户风险比例

    公式:
    qty = (account_balance * risk_per_trade) / (entry_price * stop_loss_pct)
    然后根据信号强度和市场状态调整
    """

    def __init__(self, account_balance=100, risk_per_trade=0.02,
                 base_qty=0.005, min_qty=0.001, max_qty=0.01,
                 leverage=3):
        self.account_balance = account_balance
        self.risk_per_trade = risk_per_trade
        self.base_qty = base_qty
        self.min_qty = min_qty
        self.max_qty = max_qty
        self.leverage = leverage

    def calculate(self, entry_price, stop_loss_pct, signal_count,
                  regime, qty_multiplier=1.0):
        """
        计算仓位大小

        Args:
            entry_price: 入场价格
            stop_loss_pct: 止损百分比
            signal_count: 同方向信号数量
            regime: 市场状态
            qty_multiplier: 自适应参数的仓位系数

        Returns:
            float: 仓位大小 (BTC)
        """
        if entry_price <= 0 or stop_loss_pct <= 0:
            return self.min_qty

        # 基础风控仓位: 限制单笔亏损在 risk_per_trade 以内
        risk_amount = self.account_balance * self.risk_per_trade * self.leverage
        risk_per_unit = entry_price * stop_loss_pct
        risk_qty = risk_amount / risk_per_unit if risk_per_unit > 0 else self.base_qty

        # 信号强度调整
        signal_mult = {
            1: 0.5,   # 1 个信号 → 半仓
            2: 0.8,   # 2 个信号 → 八成仓
            3: 1.0,   # 3 个信号 → 满仓
            4: 1.2,   # 4+ 信号 → 超配
        }
        sig_mult = signal_mult.get(min(signal_count, 4), 1.0)

        # 突破状态可以适当加仓
        regime_mult = {
            MarketRegimeDetector.REGIME_TRENDING_UP: 1.0,
            MarketRegimeDetector.REGIME_TRENDING_DOWN: 1.0,
            MarketRegimeDetector.REGIME_RANGING: 0.8,
            MarketRegimeDetector.REGIME_BREAKOUT: 1.2,
            MarketRegimeDetector.REGIME_LOW_VOL: 0.5,
        }
        r_mult = regime_mult.get(regime, 1.0)

        # 综合计算
        qty = self.base_qty * sig_mult * r_mult * qty_multiplier

        # 风控上限
        qty = min(qty, risk_qty)

        # 硬性限制
        qty = max(self.min_qty, min(self.max_qty, round(qty, 4)))

        return qty


# ==================== 17. 交易日志系统 (Trade Journal) ====================

class TradeJournal:
    """
    交易日志系统

    记录每笔交易的完整上下文:
    - 入场时的所有信号值
    - 市场状态
    - 仓位计算依据
    - 入场后的价格路径
    - 出场原因和 PnL

    用途:
    - 复盘分析
    - 参数优化
    - 胜率统计
    """

    def __init__(self, journal_file=None):
        self.journal_file = journal_file or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "trade_journal.jsonl"
        )

    def log_entry(self, trade_id, direction, entry_price, qty,
                  stop_loss, take_profit, signals, regime, params, reason):
        """记录入场"""
        import json

        record = {
            "type": "entry",
            "trade_id": trade_id,
            "timestamp": time.time(),
            "time_str": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "direction": direction,
            "entry_price": entry_price,
            "qty": qty,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "signals": signals,
            "regime": regime,
            "params": params,
            "reason": reason,
        }

        self._append(record)

    def log_exit(self, trade_id, exit_price, exit_reason, pnl, pnl_pct, hold_time):
        """记录出场"""
        import json

        record = {
            "type": "exit",
            "trade_id": trade_id,
            "timestamp": time.time(),
            "time_str": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "hold_time_minutes": hold_time,
        }

        self._append(record)

    def log_signal_snapshot(self, trade_id, price, cvd, delta, poc, vah, val,
                           consensus, signals):
        """记录信号快照（入场时的市场状态）"""
        record = {
            "type": "snapshot",
            "trade_id": trade_id,
            "timestamp": time.time(),
            "price": price,
            "cvd": cvd,
            "delta": delta,
            "poc": poc,
            "vah": vah,
            "val": val,
            "consensus": consensus,
            "active_signals": signals,
        }

        self._append(record)

    def get_stats(self, last_n=50):
        """获取交易统计"""
        trades = self._load_entries_and_exits()
        if not trades:
            return {"total": 0, "wins": 0, "losses": 0, "win_rate": 0, "avg_pnl": 0}

        completed = [t for t in trades if t.get("type") == "exit"]
        if not completed:
            return {"total": 0, "wins": 0, "losses": 0, "win_rate": 0, "avg_pnl": 0}

        recent = completed[-last_n:]
        wins = sum(1 for t in recent if t.get("pnl", 0) > 0)
        losses = sum(1 for t in recent if t.get("pnl", 0) <= 0)
        total_pnl = sum(t.get("pnl", 0) for t in recent)
        avg_pnl = total_pnl / len(recent) if recent else 0

        return {
            "total": len(recent),
            "wins": wins,
            "losses": losses,
            "win_rate": wins / len(recent) if recent else 0,
            "avg_pnl": avg_pnl,
            "total_pnl": total_pnl,
        }

    def _append(self, record):
        """追加记录到文件"""
        import json
        try:
            with open(self.journal_file, "a") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"⚠️ 日志写入失败: {e}")

    def _load_entries_and_exits(self):
        """加载所有入场和出场记录"""
        import json
        records = []
        try:
            with open(self.journal_file) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
        except FileNotFoundError:
            pass
        return [r for r in records if r.get("type") in ("entry", "exit")]


# ==================== 增强版信号引擎 ====================

class EnhancedSignalEngine(OrderFlowSignalEngine):
    """
    增强版信号引擎

    在基础引擎之上集成:
    - 市场状态识别
    - ATR 波动率
    - 订单簿失衡
    - 多时间框架确认
    - OI + 资金费率
    - 自适应参数
    - 动态仓位
    """

    def __init__(self, tick_size=0.1):
        super().__init__(tick_size)

        # 新增模块
        self.regime_detector = MarketRegimeDetector()
        self.atr = ATRCalculator()
        self.book_imbalance = BookImbalance()
        self.mtf = MultiTimeframeConfirm()
        self.oi_funding = OIFundingAnalyzer()
        self.adaptive = AdaptiveParams()
        self.position_sizer = DynamicPositionSizer()
        self.journal = TradeJournal()

        # 缓存
        self._regime_cache = None
        self._mtf_cache = None
        self._book_cache = None

    def feed(self, trades):
        """喂入数据，更新所有指标包括新增模块"""
        super().feed(trades)
        self.atr.update(trades)

    def analyze_enhanced(self, trades, depth_data=None, symbol="BTCUSDT"):
        """
        运行增强版分析

        Args:
            trades: aggTrades
            depth_data: 订单簿深度（可选）
            symbol: 交易对

        Returns:
            dict: 包含所有分析结果
        """
        # 基础分析
        all_signals = self.analyze(trades)
        consensus, confidence = self.get_consensus()
        current_price = float(trades[-1]["p"]) if trades else 0

        # 1. 市场状态
        self._regime_cache = self.regime_detector.detect(
            self.delta.history, self.volume_profile, current_price, trades
        )

        # 2. 订单簿失衡
        if depth_data:
            self._book_cache = self.book_imbalance.analyze(depth_data)
        else:
            self._book_cache = {"ratio": 1.0, "bias": "neutral"}

        # 3. 多时间框架
        self._mtf_cache = self.mtf.analyze(trades)

        # 4. ATR
        atr_pct = self.atr.get_atr_pct(current_price)

        # 5. 自适应参数
        adaptive_params = self.adaptive.get_params(atr_pct, self._regime_cache["regime"])

        # 6. 信号加权（加入新因子）
        enhanced_consensus, enhanced_confidence = self._calc_enhanced_consensus(
            consensus, confidence, self._regime_cache, self._mtf_cache, self._book_cache
        )

        return {
            "price": current_price,
            "signals": all_signals,
            "consensus": enhanced_consensus,
            "confidence": enhanced_confidence,
            "regime": self._regime_cache,
            "mtf": self._mtf_cache,
            "book": self._book_cache,
            "atr_pct": atr_pct,
            "adaptive_params": adaptive_params,
        }

    def _calc_enhanced_consensus(self, base_consensus, base_confidence,
                                  regime, mtf, book):
        """综合所有因子计算增强版共识"""
        # 基础分数
        score = 0
        if base_consensus == "bullish":
            score = base_confidence * 50
        elif base_consensus == "bearish":
            score = -base_confidence * 50

        # 多时间框架加权
        mtf_score = mtf.get("score", 0) * 15  # 最大 ±45

        # 订单簿加权
        book_bias = book.get("bias", "neutral")
        book_score = 0
        if book_bias == "bullish":
            book_score = 10
        elif book_bias == "bearish":
            book_score = -10

        # 市场状态调整
        regime_regime = regime.get("regime", "ranging")
        regime_adj = {
            MarketRegimeDetector.REGIME_TRENDING_UP: 1.2,
            MarketRegimeDetector.REGIME_TRENDING_DOWN: 1.2,
            MarketRegimeDetector.REGIME_RANGING: 0.8,
            MarketRegimeDetector.REGIME_BREAKOUT: 1.3,
            MarketRegimeDetector.REGIME_LOW_VOL: 0.5,
        }
        mult = regime_adj.get(regime_regime, 1.0)

        total_score = (score + mtf_score + book_score) * mult

        # 转换回共识
        if total_score > 30:
            return "bullish", min(0.95, abs(total_score) / 100)
        elif total_score < -30:
            return "bearish", min(0.95, abs(total_score) / 100)
        else:
            return "neutral", max(0, 1 - abs(total_score) / 30)


# ==================== 快捷函数 ====================

def run_analysis(symbol="BTCUSDT", minutes=30):
    """
    快速运行完整订单流分析
    
    Args:
        symbol: 交易对
        minutes: 分析最近 N 分钟的数据
    """
    print(f"🔍 正在采集 {symbol} 最近 {minutes} 分钟的成交数据...")
    
    trades = fetch_aggtrades_full(symbol=symbol, minutes=minutes)
    if not trades:
        print("❌ 无法获取数据")
        return
    
    print(f"✅ 获取到 {len(trades)} 笔成交")
    
    # 初始化引擎
    engine = OrderFlowSignalEngine(tick_size=1.0)  # BTC 用 1.0 tick
    
    # 喂入数据
    engine.feed(trades)
    
    # 运行分析
    engine.analyze(trades)
    
    # 打印报告
    return engine.print_full_report()


if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    minutes = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    run_analysis(symbol, minutes)
