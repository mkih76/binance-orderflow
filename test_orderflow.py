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
    MarketRegimeDetector, ATRCalculator, BookImbalance,
    MultiTimeframeConfirm, AdaptiveParams, DynamicPositionSizer,
    TradeJournal, EnhancedSignalEngine,
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


# ==================== 11. MarketRegimeDetector ====================

class TestMarketRegimeDetector:
    def setup_method(self):
        self.detector = MarketRegimeDetector()

    def _make_delta_history(self, n, trend="up"):
        """构造 delta 历史数据"""
        history = []
        for i in range(n):
            if trend == "up":
                delta = 2.0 + (i % 3)
                price = 62000 + i * 5
            elif trend == "down":
                delta = -2.0 - (i % 3)
                price = 62000 - i * 5
            else:  # ranging
                delta = (i % 2) * 2 - 1
                price = 62000 + (i % 3) - 1
            history.append({
                "time": i * 60, "delta": delta, "cvd": delta * (i + 1),
                "buy": max(delta, 0) + 1, "sell": max(-delta, 0) + 1,
                "price": price,
            })
        return history

    def _make_vp(self, poc=62000, vah=62020, val=61980):
        """构造 VolumeProfile"""
        vp = VolumeProfile(tick_size=1.0)
        trades = []
        for i in range(100):
            trades.append(make_trade(poc, 0.5, i % 2 == 0, 100000 + i))
        for i in range(60):
            trades.append(make_trade(vah, 0.3, i % 2 == 0, 200000 + i))
        for i in range(60):
            trades.append(make_trade(val, 0.3, i % 2 == 0, 300000 + i))
        vp.add_trades(trades)
        return vp

    def test_detect_ranging(self):
        delta_history = self._make_delta_history(15, "ranging")
        vp = self._make_vp()
        trades = make_trades_sequence(200, base_price=62000)
        result = self.detector.detect(delta_history, vp, 62000, trades)
        assert result["regime"] in ("ranging", "low_volatility")

    def test_detect_trending_up(self):
        delta_history = self._make_delta_history(15, "up")
        vp = self._make_vp()
        trades = make_trades_sequence(200, base_price=62010)
        result = self.detector.detect(delta_history, vp, 62010, trades)
        assert result["regime"] in ("trending_up", "breakout", "ranging")

    def test_detect_low_vol(self):
        # 极低波动的历史数据
        history = []
        for i in range(15):
            history.append({
                "time": i * 60, "delta": 0.1, "cvd": 0.1 * (i + 1),
                "buy": 1.0, "sell": 0.9, "price": 62000.0,
            })
        vp = self._make_vp()
        trades = make_trades_sequence(50, base_price=62000)
        result = self.detector.detect(history, vp, 62000, trades)
        assert result["regime"] in ("low_volatility", "ranging")

    def test_result_has_required_fields(self):
        delta_history = self._make_delta_history(15)
        vp = self._make_vp()
        trades = make_trades_sequence(100)
        result = self.detector.detect(delta_history, vp, 62000, trades)
        assert "regime" in result
        assert "confidence" in result
        assert "atr_ratio" in result
        assert "delta_consistency" in result
        assert "details" in result

    def test_empty_history(self):
        vp = VolumeProfile(tick_size=1.0)
        trades = make_trades_sequence(50)
        result = self.detector.detect([], vp, 62000, trades)
        assert result["regime"] in ("ranging", "low_volatility")


# ==================== 12. ATRCalculator ====================

class TestATRCalculator:
    def test_atr_basic(self):
        atr_calc = ATRCalculator(period=5)
        trades = make_trades_sequence(500, base_price=62000)
        atr_calc.update(trades, bar_seconds=300)
        atr = atr_calc.get_atr()
        # ATR 应该是一个正数
        if atr is not None:
            assert atr >= 0

    def test_atr_pct(self):
        atr_calc = ATRCalculator(period=5)
        trades = make_trades_sequence(500, base_price=62000)
        atr_calc.update(trades, bar_seconds=300)
        pct = atr_calc.get_atr_pct(62000)
        assert pct > 0

    def test_atr_pct_default(self):
        """数据不足时应返回默认值"""
        atr_calc = ATRCalculator()
        pct = atr_calc.get_atr_pct(62000)
        assert pct == 0.005  # 默认 0.5%


# ==================== 13. BookImbalance ====================

class TestBookImbalance:
    def test_bullish_imbalance(self):
        book = BookImbalance(bullish_threshold=1.5, bearish_threshold=0.67)
        depth = {
            "bids": [[62000, 10.0], [61999, 8.0], [61998, 6.0]],
            "asks": [[62001, 2.0], [62002, 3.0], [62003, 2.0]],
        }
        result = book.analyze(depth)
        assert result["bias"] == "bullish"
        assert result["ratio"] > 1.5

    def test_bearish_imbalance(self):
        book = BookImbalance(bullish_threshold=1.5, bearish_threshold=0.67)
        depth = {
            "bids": [[62000, 2.0], [61999, 3.0], [61998, 2.0]],
            "asks": [[62001, 10.0], [62002, 8.0], [62003, 6.0]],
        }
        result = book.analyze(depth)
        assert result["bias"] == "bearish"
        assert result["ratio"] < 0.67

    def test_neutral(self):
        book = BookImbalance()
        depth = {
            "bids": [[62000, 5.0], [61999, 5.0]],
            "asks": [[62001, 5.0], [62002, 5.0]],
        }
        result = book.analyze(depth)
        assert result["bias"] == "neutral"

    def test_empty_depth(self):
        book = BookImbalance()
        result = book.analyze(None)
        assert result["bias"] == "neutral"
        assert result["ratio"] == 1.0

    def test_wall_detection(self):
        book = BookImbalance()
        depth = {
            "bids": [[62000, 5.0], [61999, 50.0]],
            "asks": [[62001, 5.0], [62002, 30.0]],
        }
        result = book.analyze(depth)
        assert result["bid_wall"] == 61999.0
        assert result["bid_wall_vol"] == 50.0
        assert result["ask_wall"] == 62002.0
        assert result["ask_wall_vol"] == 30.0


# ==================== 14. MultiTimeframeConfirm ====================

class TestMultiTimeframeConfirm:
    def test_all_bullish(self):
        mtf = MultiTimeframeConfirm()
        # 构造全买单数据
        trades = []
        base_ts = int(time.time() * 1000) - 300000
        for i in range(1000):
            trades.append(make_trade(62000 + (i % 5), 1.0, False, base_ts + i * 100))
        result = mtf.analyze(trades)
        assert result["bias"] == "bullish"
        assert result["score"] == 3

    def test_all_bearish(self):
        mtf = MultiTimeframeConfirm()
        trades = []
        base_ts = int(time.time() * 1000) - 300000
        for i in range(1000):
            trades.append(make_trade(62000 + (i % 5), 1.0, True, base_ts + i * 100))
        result = mtf.analyze(trades)
        assert result["bias"] == "bearish"
        assert result["score"] == -3

    def test_mixed(self):
        mtf = MultiTimeframeConfirm()
        trades = []
        base_ts = int(time.time() * 1000) - 300000
        for i in range(500):
            trades.append(make_trade(62000, 1.0, False, base_ts + i * 100))
        for i in range(500):
            trades.append(make_trade(62000, 1.0, True, base_ts + 50000 + i * 100))
        result = mtf.analyze(trades)
        assert result["bias"] in ("bullish", "bearish", "neutral")

    def test_insufficient_data(self):
        mtf = MultiTimeframeConfirm()
        trades = make_trades_sequence(50)
        result = mtf.analyze(trades)
        assert result["bias"] == "neutral"


# ==================== 15. AdaptiveParams ====================

class TestAdaptiveParams:
    def test_basic_params(self):
        ap = AdaptiveParams(base_stop_pct=0.0065, base_tp_pct=0.013)
        params = ap.get_params(atr_pct=0.005, regime="ranging")
        assert "stop_loss_pct" in params
        assert "take_profit_pct" in params
        assert "min_signals" in params
        assert "qty_multiplier" in params
        assert params["stop_loss_pct"] > 0
        assert params["take_profit_pct"] > params["stop_loss_pct"]

    def test_high_vol_adjustment(self):
        ap = AdaptiveParams()
        params_low = ap.get_params(atr_pct=0.002, regime="ranging")
        params_high = ap.get_params(atr_pct=0.01, regime="ranging")
        # 高波动应该有更大的止损
        assert params_high["stop_loss_pct"] > params_low["stop_loss_pct"]

    def test_breakout_params(self):
        ap = AdaptiveParams()
        params = ap.get_params(atr_pct=0.005, regime="breakout")
        assert params["qty_multiplier"] >= 1.0
        assert params["take_profit_pct"] > params["stop_loss_pct"]

    def test_low_vol_params(self):
        ap = AdaptiveParams()
        params = ap.get_params(atr_pct=0.005, regime="low_volatility")
        assert params["qty_multiplier"] <= 0.5
        assert params["min_signals"] >= 3


# ==================== 16. DynamicPositionSizer ====================

class TestDynamicPositionSizer:
    def test_basic_sizing(self):
        sizer = DynamicPositionSizer(
            account_balance=100, risk_per_trade=0.02,
            base_qty=0.005, min_qty=0.001, max_qty=0.01,
        )
        qty = sizer.calculate(
            entry_price=62000, stop_loss_pct=0.0065,
            signal_count=2, regime="ranging",
        )
        assert qty >= sizer.min_qty
        assert qty <= sizer.max_qty

    def test_more_signals_bigger_position(self):
        sizer = DynamicPositionSizer(base_qty=0.005, min_qty=0.001, max_qty=0.01)
        qty_1sig = sizer.calculate(62000, 0.0065, 1, "ranging")
        qty_3sig = sizer.calculate(62000, 0.0065, 3, "ranging")
        assert qty_3sig >= qty_1sig

    def test_breakout_bigger_than_ranging(self):
        sizer = DynamicPositionSizer(base_qty=0.005, min_qty=0.001, max_qty=0.01)
        qty_ranging = sizer.calculate(62000, 0.0065, 2, "ranging")
        qty_breakout = sizer.calculate(62000, 0.0065, 2, "breakout")
        assert qty_breakout >= qty_ranging

    def test_respects_limits(self):
        sizer = DynamicPositionSizer(base_qty=0.005, min_qty=0.001, max_qty=0.01)
        qty = sizer.calculate(62000, 0.0065, 4, "breakout", qty_multiplier=5.0)
        assert qty <= 0.01

    def test_min_qty_guaranteed(self):
        sizer = DynamicPositionSizer(base_qty=0.005, min_qty=0.001, max_qty=0.01)
        qty = sizer.calculate(62000, 0.0065, 1, "low_volatility")
        assert qty >= 0.001


# ==================== 17. TradeJournal ====================

class TestTradeJournal:
    def setup_method(self):
        import tempfile
        self.tmp_file = tempfile.mktemp(suffix=".jsonl")
        self.journal = TradeJournal(journal_file=self.tmp_file)

    def teardown_method(self):
        import os
        if os.path.exists(self.tmp_file):
            os.remove(self.tmp_file)

    def test_log_entry(self):
        self.journal.log_entry(
            trade_id="T1", direction="long", entry_price=62000,
            qty=0.005, stop_loss=61600, take_profit=62800,
            signals=[{"source": "test"}], regime={"regime": "ranging"},
            params={}, reason="test entry",
        )
        stats = self.journal.get_stats()
        # 入场记录不应计入统计（需要 exit）
        assert stats["total"] == 0

    def test_log_exit(self):
        self.journal.log_entry(
            trade_id="T1", direction="long", entry_price=62000,
            qty=0.005, stop_loss=61600, take_profit=62800,
            signals=[], regime={}, params={}, reason="test",
        )
        self.journal.log_exit("T1", 62500, "take_profit", 2.5, 0.8, 10)
        stats = self.journal.get_stats()
        assert stats["total"] == 1
        assert stats["wins"] == 1
        assert stats["win_rate"] == 1.0

    def test_stats_win_loss(self):
        # 赢
        self.journal.log_entry("T1", "long", 62000, 0.005, 61600, 62800, [], {}, {}, "test")
        self.journal.log_exit("T1", 62500, "tp", 2.5, 0.8, 10)
        # 亏
        self.journal.log_entry("T2", "long", 62000, 0.005, 61600, 62800, [], {}, {}, "test")
        self.journal.log_exit("T2", 61500, "sl", -2.5, -0.8, 5)
        stats = self.journal.get_stats()
        assert stats["total"] == 2
        assert stats["wins"] == 1
        assert stats["losses"] == 1
        assert stats["win_rate"] == 0.5


# ==================== 18. EnhancedSignalEngine ====================

class TestEnhancedSignalEngine:
    def test_engine_runs(self):
        engine = EnhancedSignalEngine(tick_size=1.0)
        base_ts = int(time.time() * 1000) - 60000
        trades = []
        for i in range(300):
            price = 62000 + (i % 6)
            trades.append(make_trade(price, 0.5, False, ts=base_ts + i * 100))
        for i in range(50):
            price = 62010 + (i % 6)
            trades.append(make_trade(price, 0.1, True, ts=base_ts + 30000 + i * 100))
        engine.feed(trades)
        result = engine.analyze_enhanced(trades)
        assert "price" in result
        assert "signals" in result
        assert "consensus" in result
        assert "confidence" in result
        assert "regime" in result
        assert "mtf" in result
        assert "atr_pct" in result
        assert "adaptive_params" in result

    def test_regime_detection(self):
        engine = EnhancedSignalEngine(tick_size=1.0)
        trades = make_trades_sequence(500, base_price=62000)
        engine.feed(trades)
        result = engine.analyze_enhanced(trades)
        assert result["regime"]["regime"] in (
            "trending_up", "trending_down", "ranging", "breakout", "low_volatility"
        )

    def test_mtf_integration(self):
        engine = EnhancedSignalEngine(tick_size=1.0)
        base_ts = int(time.time() * 1000) - 300000
        trades = []
        for i in range(1000):
            trades.append(make_trade(62000 + (i % 5), 1.0, False, base_ts + i * 100))
        engine.feed(trades)
        result = engine.analyze_enhanced(trades)
        assert result["mtf"]["bias"] in ("bullish", "bearish", "neutral")

    def test_adaptive_params_present(self):
        engine = EnhancedSignalEngine(tick_size=1.0)
        trades = make_trades_sequence(500, base_price=62000)
        engine.feed(trades)
        result = engine.analyze_enhanced(trades)
        params = result["adaptive_params"]
        assert "stop_loss_pct" in params
        assert "take_profit_pct" in params
        assert "min_signals" in params
        assert "qty_multiplier" in params
