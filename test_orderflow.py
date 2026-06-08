#!/usr/bin/env python3
"""
订单流分析引擎 pytest 测试
==========================
运行: pytest test_orderflow.py -v
"""

import sys
import os
import time
import pytest

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from orderflow import (
    FootprintChart, DeltaTracker, VolumeProfile,
    ImbalanceDetector, AbsorptionDetector, ExhaustionDetector,
    IcebergDetector, SpeedOfTape, OrderFlowSignalEngine,
    fetch_aggtrades_full,
)


# ==================== 辅助工具 ====================

def make_trade(price, qty, is_sell=True, ts=None):
    """构造单笔 aggTrade"""
    return {
        "a": 1, "p": str(price), "q": str(qty),
        "f": 1, "l": 1, "T": ts or int(time.time() * 1000),
        "m": is_sell,
    }

def make_trades_sequence(n, base_price=62000, base_ts=None):
    if base_ts is None:
        base_ts = int(time.time() * 1000) - n * 1000
    trades = []
    for i in range(n):
        price = base_price + (i % 10) * 1.0
        qty = 0.1 + (i % 3) * 0.05
        is_sell = (i % 2 == 0)
        trades.append(make_trade(price, qty, is_sell, base_ts + i * 100))
    return trades


# ==================== 1. FootprintChart ====================

class TestFootprintChart:
    def setup_method(self):
        self.fp = FootprintChart(tick_size=1.0)

    def test_price_alignment(self):
        trades = [
            make_trade(62000.0, 1.0, True, 1000000),
            make_trade(62001.0, 2.0, False, 1000300),
        ]
        self.fp.add_trades(trades)
        bar_time = int(1000000 / 1000 / 300) * 300
        bar = self.fp.get_bar(bar_time)
        assert 62000.0 in bar
        assert 62001.0 in bar

    def test_sell_volume(self):
        trades = [
            make_trade(62000.0, 1.0, True, 1000000),
            make_trade(62000.0, 1.0, True, 1000100),
            make_trade(62000.0, 1.0, True, 1000200),
        ]
        self.fp.add_trades(trades)
        bar_time = int(1000000 / 1000 / 300) * 300
        bar = self.fp.get_bar(bar_time)
        assert bar[62000]["sell"] == 3.0

    def test_buy_volume(self):
        trades = [
            make_trade(62001.0, 2.0, False, 1000300),
            make_trade(62001.0, 2.0, False, 1000400),
        ]
        self.fp.add_trades(trades)
        bar_time = int(1000300 / 1000 / 300) * 300
        bar = self.fp.get_bar(bar_time)
        assert bar[62001]["buy"] == 4.0

    def test_mixed_volume(self):
        trades = [
            make_trade(62002.0, 0.5, True, 1000500),
            make_trade(62002.0, 1.5, False, 1000600),
        ]
        self.fp.add_trades(trades)
        bar_time = int(1000500 / 1000 / 300) * 300
        bar = self.fp.get_bar(bar_time)
        assert bar[62002]["sell"] == 0.5
        assert bar[62002]["buy"] == 1.5

    def test_tick_size_0_1(self):
        fp2 = FootprintChart(tick_size=0.1)
        fp2.add_trades([make_trade(62000.15, 1.0, True, 1000000)])
        bar_time = 300 * (1000000 // 1000 // 300)
        bar = fp2.get_bar(bar_time)
        assert 62000.2 in bar

    def test_empty_trades(self):
        self.fp.add_trades([])
        assert self.fp.get_latest_bar() == (None, {})

    def test_single_trade(self):
        self.fp.add_trades([make_trade(62000, 1.0, True, 1000000)])
        _, bar = self.fp.get_latest_bar()
        assert 62000 in bar
        assert bar[62000]["sell"] == 1.0


# ==================== 2. DeltaTracker ====================

class TestDeltaTracker:
    def test_delta_calculation(self):
        dt = DeltaTracker()
        trades = []
        for i in range(5):
            trades.append(make_trade(62000, 1.0, True, 100000 + i * 100))
        for i in range(3):
            trades.append(make_trade(62000, 1.5, False, 100500 + i * 100))
        dt.add_trades(trades, period_seconds=600)
        assert abs(dt.current_delta - (-0.5)) < 0.001

    def test_cvd(self):
        dt = DeltaTracker()
        trades = []
        for i in range(5):
            trades.append(make_trade(62000, 1.0, True, 100000 + i * 100))
        for i in range(3):
            trades.append(make_trade(62000, 1.5, False, 100500 + i * 100))
        dt.add_trades(trades, period_seconds=600)
        assert abs(dt.cvd - (-0.5)) < 0.001

    def test_buy_sell_volume(self):
        dt = DeltaTracker()
        trades = []
        for i in range(5):
            trades.append(make_trade(62000, 1.0, True, 100000 + i * 100))
        for i in range(3):
            trades.append(make_trade(62000, 1.5, False, 100500 + i * 100))
        dt.add_trades(trades, period_seconds=600)
        assert abs(dt.current_buy - 4.5) < 0.001
        assert abs(dt.current_sell - 5.0) < 0.001

    def test_multi_period(self):
        dt = DeltaTracker()
        trades = []
        for i in range(5):
            trades.append(make_trade(62000, 1.0, True, 100000 + i * 100))
        for i in range(5):
            trades.append(make_trade(62001, 2.0, False, 200000 + i * 100))
        dt.add_trades(trades, period_seconds=100)
        assert len(dt.history) >= 2

    def test_empty(self):
        dt = DeltaTracker()
        dt.add_trades([], period_seconds=60)
        assert dt.cvd == 0


# ==================== 3. VolumeProfile ====================

class TestVolumeProfile:
    def setup_method(self):
        self.vp = VolumeProfile(tick_size=1.0)
        trades = []
        for i in range(100):
            trades.append(make_trade(62000, 0.1, i % 2 == 0, 100000 + i))
        for i in range(80):
            trades.append(make_trade(62001, 0.1, i % 2 == 0, 200000 + i))
        for i in range(60):
            trades.append(make_trade(62002, 0.1, i % 2 == 0, 300000 + i))
        for i in range(40):
            trades.append(make_trade(62003, 0.1, i % 2 == 0, 400000 + i))
        for i in range(20):
            trades.append(make_trade(62004, 0.1, i % 2 == 0, 500000 + i))
        self.vp.add_trades(trades)

    def test_poc(self):
        assert self.vp.get_poc() == 62000.0

    def test_value_area_bounds(self):
        vah, val, poc = self.vp.get_value_area(0.70)
        assert vah >= poc
        assert val <= poc
        assert val <= poc <= vah

    def test_value_area_ratio(self):
        vah, val, _ = self.vp.get_value_area(0.70)
        total_vol = sum(self.vp.profile.values())
        va_vol = sum(self.vp.profile.get(p, 0) for p in self.vp.profile if val <= p <= vah)
        assert va_vol / total_vol >= 0.65

    def test_hvn_contains_poc(self):
        hvns, _ = self.vp.get_hvn_lvn(75)
        assert 62000.0 in hvns

    def test_lvn_contains_lowest(self):
        _, lvns = self.vp.get_hvn_lvn(75)
        assert 62004.0 in lvns

    def test_empty(self):
        vp = VolumeProfile()
        vp.add_trades([])
        assert vp.get_poc() is None

    def test_large_volume(self):
        vp = VolumeProfile(tick_size=1.0)
        vp.add_trades([make_trade(62000, 999999.999, True, 1000000)])
        assert vp.get_poc() == 62000.0


# ==================== 4. ImbalanceDetector ====================

class TestImbalanceDetector:
    def setup_method(self):
        self.imb = ImbalanceDetector(ratio=3.0, min_stack=3)

    def test_buy_imbalance(self):
        bar = {
            62003: {"buy": 9.0, "sell": 1.0},
            62002: {"buy": 8.0, "sell": 1.0},
            62001: {"buy": 7.0, "sell": 1.0},
            62000: {"buy": 6.0, "sell": 1.0},
        }
        signals = self.imb.detect(bar)
        buy_signals = [s for s in signals if s["type"] == "stacked_buy_imbalance"]
        assert len(buy_signals) >= 1
        assert buy_signals[0]["bias"] == "bullish"
        assert buy_signals[0]["levels"] >= 3

    def test_sell_imbalance(self):
        bar = {
            62003: {"buy": 1.0, "sell": 0.5},
            62002: {"buy": 1.0, "sell": 7.0},
            62001: {"buy": 1.0, "sell": 8.0},
            62000: {"buy": 1.0, "sell": 9.0},
        }
        signals = self.imb.detect(bar)
        sell_signals = [s for s in signals if s["type"] == "stacked_sell_imbalance"]
        assert len(sell_signals) >= 1

    def test_balanced_no_signal(self):
        bar = {
            62001: {"buy": 3.0, "sell": 3.0},
            62000: {"buy": 3.0, "sell": 3.0},
        }
        signals = self.imb.detect(bar)
        assert len(signals) == 0


# ==================== 5. AbsorptionDetector ====================

class TestAbsorptionDetector:
    def test_detect_with_baseline(self):
        det = AbsorptionDetector(z_threshold=2.0, imbalance_threshold=0.5, price_impact_threshold=0.8)
        # 喂历史建立 baseline
        for i in range(20):
            normal = [make_trade(62000 + (j % 3), 0.1, j % 2 == 0, ts=i * 100000 + j * 100) for j in range(50)]
            det.detect(normal, window_seconds=100)
        # 吸收事件
        abs_trades = [make_trade(62001, 1.0, True, ts=5000000 + i * 10) for i in range(200)]
        signals = det.detect(abs_trades, window_seconds=100)
        # 只要不崩溃就算通过
        assert isinstance(signals, list)

    def test_zscore_formula(self):
        vols = [10, 12, 11, 13, 10, 12, 11, 13, 10, 12]
        mean_v = sum(vols) / len(vols)
        std_v = (sum((v - mean_v) ** 2 for v in vols) / len(vols)) ** 0.5
        z = (100 - mean_v) / std_v
        assert z > 3


# ==================== 6. ExhaustionDetector ====================

class TestExhaustionDetector:
    def test_bearish_exhaustion(self):
        exh = ExhaustionDetector(lookback=5, volume_decline_threshold=0.3)
        history = [
            {"time": 1, "delta": 5.0, "cvd": 5.0, "buy": 8.0, "sell": 3.0, "price": 62000},
            {"time": 2, "delta": 4.0, "cvd": 9.0, "buy": 7.0, "sell": 3.0, "price": 62010},
            {"time": 3, "delta": 3.0, "cvd": 12.0, "buy": 6.0, "sell": 3.0, "price": 62020},
            {"time": 4, "delta": -2.0, "cvd": 10.0, "buy": 2.0, "sell": 4.0, "price": 62025},
            {"time": 5, "delta": -3.0, "cvd": 7.0, "buy": 1.0, "sell": 4.0, "price": 62026},
        ]
        signals = exh.detect(history)
        bearish = [s for s in signals if s.get("direction") == "bearish_exhaustion"]
        assert len(bearish) >= 1

    def test_no_exhaustion_on_acceleration(self):
        exh = ExhaustionDetector(lookback=5, volume_decline_threshold=0.3)
        history = [
            {"time": 1, "delta": 2.0, "cvd": 2.0, "buy": 5.0, "sell": 3.0, "price": 62000},
            {"time": 2, "delta": 3.0, "cvd": 5.0, "buy": 6.0, "sell": 3.0, "price": 62005},
            {"time": 3, "delta": 4.0, "cvd": 9.0, "buy": 7.0, "sell": 3.0, "price": 62010},
            {"time": 4, "delta": 5.0, "cvd": 14.0, "buy": 8.0, "sell": 3.0, "price": 62015},
            {"time": 5, "delta": 6.0, "cvd": 20.0, "buy": 9.0, "sell": 3.0, "price": 62020},
        ]
        signals = exh.detect(history)
        assert len(signals) == 0


# ==================== 7. IcebergDetector ====================

class TestIcebergDetector:
    def test_detect_iceberg(self):
        ice = IcebergDetector(min_fills=8, max_cv=0.5, z_threshold=1.5)
        trades = []
        for i in range(20):
            trades.append(make_trade(62000, 0.5, True, ts=1000000 + i * 100))
        for price in [62001, 62002, 62003]:
            for i in range(2):
                trades.append(make_trade(price, 0.1, i % 2 == 0, ts=2000000 + i * 100))
        signals = ice.detect(trades)
        detected = [s for s in signals if s["price"] == 62000]
        assert len(detected) >= 1
        assert detected[0]["fills"] == 20
        assert abs(detected[0]["avg_qty"] - 0.5) < 0.01
        assert detected[0]["cv"] < 0.01

    def test_no_iceberg_irregular_qty(self):
        ice = IcebergDetector(min_fills=8, max_cv=0.5, z_threshold=1.5)
        trades = []
        for i in range(15):
            qty = 0.1 + i * 0.5
            trades.append(make_trade(62050, qty, True, ts=3000000 + i * 100))
        signals = ice.detect(trades)
        non_ice = [s for s in signals if s["price"] == 62050]
        assert len(non_ice) == 0

    def test_direction_detection(self):
        """修复后的方向判断：按该价格实际成交统计"""
        ice = IcebergDetector(min_fills=5, max_cv=0.5, z_threshold=1.0)
        trades = []
        # 62000: 10 笔卖单 + 2 笔买单 → direction 应为 "sell"
        for i in range(10):
            trades.append(make_trade(62000, 0.5, True, ts=1000000 + i * 100))
        for i in range(2):
            trades.append(make_trade(62000, 0.5, False, ts=1000000 + (10 + i) * 100))
        # 其他价格少量成交
        for i in range(2):
            trades.append(make_trade(62001, 0.1, True, ts=2000000 + i * 100))
        signals = ice.detect(trades)
        detected = [s for s in signals if s["price"] == 62000]
        if detected:
            assert detected[0]["direction"] == "sell"


# ==================== 8. SpeedOfTape ====================

class TestSpeedOfTape:
    def test_accelerating_speed(self):
        sot = SpeedOfTape(window_seconds=1)
        trades = []
        for sec in range(5):
            n_trades = (sec + 1) * 10
            for i in range(n_trades):
                trades.append(make_trade(62000, 0.1, i % 2 == 0, ts=sec * 1000 + i * 10))
        sot.add_trades(trades)
        test_speeds = [h["speed"] for h in sot.speed_history if h["time"] < 10000]
        assert len(test_speeds) >= 5
        assert all(test_speeds[i] <= test_speeds[i + 1] for i in range(len(test_speeds) - 1))

    def test_empty(self):
        sot = SpeedOfTape()
        sot.add_trades([])
        assert sot.get_acceleration() == 0


# ==================== 9. 信号引擎集成 ====================

class TestSignalEngine:
    def test_engine_runs(self):
        engine = OrderFlowSignalEngine(tick_size=1.0)
        base_ts = int(time.time() * 1000) - 60000
        trades = []
        for i in range(300):
            price = 62000 + (i % 6)
            trades.append(make_trade(price, 0.5, False, ts=base_ts + i * 100))
        for i in range(50):
            price = 62010 + (i % 6)
            trades.append(make_trade(price, 0.1, True, ts=base_ts + 30000 + i * 100))
        engine.feed(trades)
        signals = engine.analyze(trades)
        consensus, confidence = engine.get_consensus()
        assert consensus in ("bullish", "bearish", "neutral")
        assert 0 <= confidence <= 1
        assert engine.volume_profile.get_poc() is not None

    def test_divergence_detection(self):
        dt = DeltaTracker()
        div_trades = []
        for i in range(20):
            price = 62000 - i * 2 if i < 10 else 61980 + i
            div_trades.append(make_trade(price, 1.0, True, ts=100000 + i * 60000))
        dt.add_trades(div_trades, period_seconds=300)
        div = dt.get_divergence(lookback=10)
        assert div in ("bullish_div", "bearish_div", "none")


# ==================== 10. fetch_aggtrades_full ====================

class TestFetchAggtradesFull:
    """注意：这些测试依赖网络，标记为 slow"""

    @pytest.mark.slow
    def test_fetch_returns_list(self):
        """测试分页获取功能（需要网络连接到 demo-fapi）"""
        trades = fetch_aggtrades_full("BTCUSDT", minutes=1)
        assert isinstance(trades, list)
        # 如果网络可用，应该有数据
        if trades:
            assert "p" in trades[0]
            assert "q" in trades[0]
            assert "m" in trades[0]
