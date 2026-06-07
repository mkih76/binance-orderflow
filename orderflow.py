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
"""

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
        
        # Flush 最后一个周期
        if trades and self._period_start > 0:
            self.history.append({
                "time": self._period_start,
                "delta": self.current_delta,
                "cvd": self.cvd,
                "buy": self.current_buy,
                "sell": self.current_sell,
                "price": float(trades[-1]["p"]),
            })
            self.current_delta = 0.0
            self.current_buy = 0.0
            self.current_sell = 0.0
    
    def get_divergence(self, lookback=10):
        """
        CVD 背离检测
        
        看涨背离: 价格创新低，但 CVD 没有创新低
        看跌背离: 价格创新高，但 CVD 没有创新高
        
        Returns:
            str: "bullish_div" / "bearish_div" / "none"
        """
        if len(self.history) < lookback:
            return "none"
        
        recent = self.history[-lookback:]
        prices = [h["price"] for h in recent]
        cvds = [h["cvd"] for h in recent]
        
        # 找最近的两个极值点
        price_min_idx = prices.index(min(prices))
        price_max_idx = prices.index(max(prices))
        cvd_min_idx = cvds.index(min(cvds))
        cvd_max_idx = cvds.index(max(cvds))
        
        # 看涨背离: 价格低点在后，但 CVD 低点在前
        if price_min_idx > cvd_min_idx and price_min_idx == len(prices) - 1:
            return "bullish_div"
        
        # 看跌背离: 价格高点在后，但 CVD 高点在前
        if price_max_idx > cvd_max_idx and price_max_idx == len(prices) - 1:
            return "bearish_div"
        
        return "none"
    
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
            
            if up_vol >= down_vol:
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
    堆叠失衡检测
    
    原理：
    - 在足迹图中，如果某价格的买量是下方价格卖量的 N 倍以上 → 买方失衡
    - 连续 3+ 个价格出现同方向失衡 → 堆叠失衡（Stacked Imbalance）
    - 堆叠失衡 = 强势方向信号
    
    参数：
    - ratio: 失衡比率阈值（默认 3.0，即 3:1）
    - min_stack: 最少堆叠层数（默认 3）
    """
    
    def __init__(self, ratio=3.0, min_stack=3):
        self.ratio = ratio
        self.min_stack = min_stack
    
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
    吸收检测
    
    定义：大量主动成交发生，但价格几乎不动
    → 说明有大量被动限价单在吸收主动单
    
    检测公式（三个条件同时满足）：
    1. Volume Z-Score > 3.0（成交量统计显著异常）
    2. Net Taker Imbalance > 60% 或 < -60%（极端方向性）
    3. Relative Price Impact < 0.8（价格几乎没动）
    
    信号：
    - 买方吸收（卖方被吸收）→ 看涨
    - 卖方吸收（买方被吸收）→ 看跌
    """
    
    def __init__(self, z_threshold=3.0, imbalance_threshold=0.6, price_impact_threshold=0.8):
        self.z_threshold = z_threshold
        self.imbalance_threshold = imbalance_threshold
        self.price_impact_threshold = price_impact_threshold
        self.volume_history = deque(maxlen=100)  # 历史成交量用于计算 Z-Score
    
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
            avg_price = mid_price
            normal_impact = relative_impact / (avg_price * 0.001)  # 0.1% 作为基准
            normal_impact = min(normal_impact, 2.0)  # 上限
            
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
    2. 最近 N 个周期成交量递减
    3. Delta 方向与价格方向不一致
    """
    
    def __init__(self, lookback=10, volume_decline_threshold=0.5):
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
        
        # 计算成交量趋势
        if len(vols) >= 3:
            recent_vol_avg = sum(vols[-3:]) / 3
            earlier_vol_avg = sum(vols[:-3]) / max(1, len(vols) - 3)
            
            vol_decline = 1 - (recent_vol_avg / earlier_vol_avg) if earlier_vol_avg > 0 else 0
            
            if vol_decline > self.volume_decline_threshold:
                # 价格在极端位置？
                current_price = prices[-1]
                price_high = max(prices)
                price_low = min(prices)
                price_range = price_high - price_low
                
                if price_range > 0:
                    # 检查是否在高点衰竭（看跌）
                    if (price_high - current_price) / price_range < 0.2:
                        # 价格在高位，成交量萎缩 → 看跌衰竭
                        recent_delta = sum(h["delta"] for h in recent[-3:])
                        if recent_delta < 0:  # Delta 转负
                            signals.append({
                                "type": "exhaustion",
                                "direction": "bearish_exhaustion",
                                "bias": "bearish",
                                "vol_decline": vol_decline,
                                "price": current_price,
                                "recent_delta": recent_delta,
                            })
                    
                    # 检查是否在低点衰竭（看涨）
                    if (current_price - price_low) / price_range < 0.2:
                        recent_delta = sum(h["delta"] for h in recent[-3:])
                        if recent_delta > 0:  # Delta 转正
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
                    direction = "buy" if not trades[0]["m"] else "sell"  # 简化判断
                    
                    signals.append({
                        "type": "iceberg",
                        "price": price,
                        "fills": n_fills,
                        "avg_qty": mean_qty,
                        "total_volume": total_vol,
                        "cv": cv,
                        "z_score": z_score,
                        "direction": "unknown",  # 需要更多上下文判断
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
        
        # 1. 堆叠失衡
        _, latest_bar = self.footprint.get_latest_bar()
        if latest_bar:
            imb_signals = self.imbalance.detect(latest_bar)
            for s in imb_signals:
                s["source"] = "stacked_imbalance"
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
        
        # 5. CVD 背离
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
        
        # 加权评分（吸收和堆叠失衡权重更高）
        weights = {
            "absorption": 3.0,
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


# ==================== 快捷函数 ====================

def run_analysis(symbol="BTCUSDT", minutes=30):
    """
    快速运行完整订单流分析
    
    Args:
        symbol: 交易对
        minutes: 分析最近 N 分钟的数据
    """
    print(f"🔍 正在采集 {symbol} 最近 {minutes} 分钟的成交数据...")
    
    # 计算需要的成交笔数（约 1000笔/分钟 for BTC）
    limit = min(1000, minutes * 100)
    
    trades = fetch_aggtrades(symbol=symbol, limit=limit)
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
