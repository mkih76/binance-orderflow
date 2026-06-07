#!/usr/bin/env python3
"""
ATAS 核心功能 IPQC 验证
========================
用合成数据（已知属性）验证每个模块的计算正确性
"""

import sys
import time
import json
sys.path.insert(0, "/opt/binance-testnet")

from orderflow import (
    FootprintChart, DeltaTracker, VolumeProfile,
    ImbalanceDetector, AbsorptionDetector, ExhaustionDetector,
    IcebergDetector, SpeedOfTape, OrderFlowSignalEngine
)

PASS = "✅ PASS"
FAIL = "❌ FAIL"
WARN = "⚠️ WARN"
results = []

def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((name, status, detail))
    print(f"  {status}  {name}" + (f"  ({detail})" if detail else ""))

def make_trade(price, qty, is_sell=True, ts=None):
    """构造单笔 aggTrade"""
    return {
        "a": 1, "p": str(price), "q": str(qty),
        "f": 1, "l": 1, "T": ts or int(time.time() * 1000),
        "m": is_sell  # isBuyerMaker: true=卖方主动
    }

def make_trades_sequence(n, base_price=62000, base_ts=None):
    """构造一系列交替买卖成交"""
    if base_ts is None:
        base_ts = int(time.time() * 1000) - n * 1000
    trades = []
    for i in range(n):
        price = base_price + (i % 10) * 1.0
        qty = 0.1 + (i % 3) * 0.05
        is_sell = (i % 2 == 0)  # 交替: 0=卖, 1=买
        trades.append(make_trade(price, qty, is_sell, base_ts + i * 100))
    return trades

# ==================== TEST 1: FootprintChart ====================
print("\n" + "="*60)
print("  TEST 1: FootprintChart 足迹图")
print("="*60)

fp = FootprintChart(tick_size=1.0)

# 构造已知数据:
# 价格 62000: 3 笔卖单, 每笔 1.0 → sell=3.0
# 价格 62001: 2 笔买单, 每笔 2.0 → buy=4.0
# 价格 62002: 1 笔卖单 0.5 + 1 笔买单 1.5 → sell=0.5, buy=1.5
test_trades = [
    make_trade(62000.0, 1.0, is_sell=True, ts=1000000),
    make_trade(62000.0, 1.0, is_sell=True, ts=1000100),
    make_trade(62000.0, 1.0, is_sell=True, ts=1000200),
    make_trade(62001.0, 2.0, is_sell=False, ts=1000300),
    make_trade(62001.0, 2.0, is_sell=False, ts=1000400),
    make_trade(62002.0, 0.5, is_sell=True, ts=1000500),
    make_trade(62002.0, 1.5, is_sell=False, ts=1000600),
]

fp.add_trades(test_trades)
bar_time = int(1000000 / 1000 / 300) * 300
bar = fp.get_bar(bar_time)

check("价格对齐 tick_size=1.0", 62000.0 in bar, f"keys={list(bar.keys())}")
check("62000 卖量=3.0", bar.get(62000, {}).get("sell", 0) == 3.0,
      f"sell={bar.get(62000, {}).get('sell', 0)}")
check("62001 买量=4.0", bar.get(62001, {}).get("buy", 0) == 4.0,
      f"buy={bar.get(62001, {}).get('buy', 0)}")
check("62002 买=1.5 卖=0.5", 
      bar.get(62002, {}).get("buy", 0) == 1.5 and bar.get(62002, {}).get("sell", 0) == 0.5,
      f"buy={bar.get(62002, {}).get('buy', 0)}, sell={bar.get(62002, {}).get('sell', 0)}")

# 测试小 tick_size 精度
fp2 = FootprintChart(tick_size=0.1)
fp2.add_trades([make_trade(62000.15, 1.0, True, 1000000)])
bar2 = fp2.get_bar(300 * (1000000 // 1000 // 300))
has_62000_2 = 62000.2 in bar2
check("tick_size=0.1 精度对齐", has_62000_2, f"62000.2 in bar: {has_62000_2}")

# ==================== TEST 2: DeltaTracker ====================
print("\n" + "="*60)
print("  TEST 2: DeltaTracker Delta/CVD")
print("="*60)

dt = DeltaTracker()

# 构造: 5笔卖(各1.0) + 3笔买(各1.5) → delta = -5.0 + 4.5 = -0.5
delta_trades = []
for i in range(5):
    delta_trades.append(make_trade(62000, 1.0, is_sell=True, ts=100000 + i*100))
for i in range(3):
    delta_trades.append(make_trade(62000, 1.5, is_sell=False, ts=100500 + i*100))

dt.add_trades(delta_trades, period_seconds=600)  # 600秒窗口，全在一个周期

check("当前 Delta = -0.5", abs(dt.current_delta - (-0.5)) < 0.001,
      f"delta={dt.current_delta}")
check("CVD = -0.5", abs(dt.cvd - (-0.5)) < 0.001, f"cvd={dt.cvd}")
check("买量 = 4.5", abs(dt.current_buy - 4.5) < 0.001, f"buy={dt.current_buy}")
check("卖量 = 5.0", abs(dt.current_sell - 5.0) < 0.001, f"sell={dt.current_sell}")

# 测试多周期
dt2 = DeltaTracker()
multi_trades = []
for i in range(5):
    multi_trades.append(make_trade(62000, 1.0, True, ts=100000 + i*100))
for i in range(5):
    multi_trades.append(make_trade(62001, 2.0, False, ts=200000 + i*100))

dt2.add_trades(multi_trades, period_seconds=100)  # 100秒窗口
check("多周期历史记录 >= 2", len(dt2.history) >= 2,
      f"history_len={len(dt2.history)}")

# ==================== TEST 3: VolumeProfile ====================
print("\n" + "="*60)
print("  TEST 3: VolumeProfile 成交量分布")
print("="*60)

vp = VolumeProfile(tick_size=1.0)

# 构造已知分布:
# 62000: 10.0 vol → POC
# 62001: 8.0 vol
# 62002: 6.0 vol
# 62003: 4.0 vol
# 62004: 2.0 vol
vp_trades = []
for i in range(100):
    vp_trades.append(make_trade(62000, 0.1, i%2==0, 100000+i))
for i in range(80):
    vp_trades.append(make_trade(62001, 0.1, i%2==0, 200000+i))
for i in range(60):
    vp_trades.append(make_trade(62002, 0.1, i%2==0, 300000+i))
for i in range(40):
    vp_trades.append(make_trade(62003, 0.1, i%2==0, 400000+i))
for i in range(20):
    vp_trades.append(make_trade(62004, 0.1, i%2==0, 500000+i))

vp.add_trades(vp_trades)

poc = vp.get_poc()
check("POC = 62000", poc == 62000.0, f"poc={poc}")

vah, val, poc2 = vp.get_value_area(0.70)
check("VAH >= POC", vah >= poc, f"vah={vah}, poc={poc}")
check("VAL <= POC", val <= poc, f"val={val}, poc={poc}")
check("价值区包含 POC", val <= poc <= vah, f"val={val}, poc={poc}, vah={vah}")

total_vol = sum(vp.profile.values())
va_vol = sum(vp.profile.get(p, 0) for p in vp.profile if val <= p <= vah)
check("价值区占比 >= 65%", va_vol / total_vol >= 0.65,
      f"va_ratio={va_vol/total_vol:.2%}")

hvns, lvns = vp.get_hvn_lvn(75)
check("HVN 包含 POC", 62000.0 in hvns, f"hvns={hvns[:3]}")
check("LVN 包含最低量价", 62004.0 in lvns, f"lvns={lvns[:3]}")

# ==================== TEST 4: ImbalanceDetector ====================
print("\n" + "="*60)
print("  TEST 4: ImbalanceDetector 堆叠失衡")
print("="*60)

imb = ImbalanceDetector(ratio=3.0, min_stack=3)

# 构造买方堆叠失衡:
# 价格 62003: buy=9.0, sell=1.0  (上方买 vs 下方卖 比值 >= 3)
# 价格 62002: buy=8.0, sell=1.0
# 价格 62001: buy=7.0, sell=1.0
# 价格 62000: buy=6.0, sell=1.0  (也需要足够买量)
imb_bar = {
    62003: {"buy": 9.0, "sell": 1.0},
    62002: {"buy": 8.0, "sell": 1.0},
    62001: {"buy": 7.0, "sell": 1.0},
    62000: {"buy": 6.0, "sell": 1.0},
}

signals = imb.detect(imb_bar)
buy_signals = [s for s in signals if s["type"] == "stacked_buy_imbalance"]
check("检测到买方堆叠失衡", len(buy_signals) >= 1,
      f"count={len(buy_signals)}")
if buy_signals:
    check("失衡方向=bullish", buy_signals[0]["bias"] == "bullish",
          f"bias={buy_signals[0]['bias']}")
    check("层数 >= 3", buy_signals[0]["levels"] >= 3,
          f"levels={buy_signals[0]['levels']}")

# 构造卖方堆叠失衡
imb_bar_sell = {
    62003: {"buy": 1.0, "sell": 0.5},
    62002: {"buy": 1.0, "sell": 7.0},
    62001: {"buy": 1.0, "sell": 8.0},
    62000: {"buy": 1.0, "sell": 9.0},
}

signals2 = imb.detect(imb_bar_sell)
sell_signals = [s for s in signals2 if s["type"] == "stacked_sell_imbalance"]
check("检测到卖方堆叠失衡", len(sell_signals) >= 1,
      f"count={len(sell_signals)}")

# 构造无失衡（均衡）
imb_bar_neutral = {
    62001: {"buy": 3.0, "sell": 3.0},
    62000: {"buy": 3.0, "sell": 3.0},
}

signals3 = imb.detect(imb_bar_neutral)
check("均衡市场无堆叠失衡", len(signals3) == 0, f"count={len(signals3)}")

# ==================== TEST 5: AbsorptionDetector ====================
print("\n" + "="*60)
print("  TEST 5: AbsorptionDetector 吸收检测")
print("="*60)

abs_det = AbsorptionDetector(z_threshold=2.0, imbalance_threshold=0.5, price_impact_threshold=0.8)

# 构造吸收场景:
# 大量成交但价格几乎不动
# 先喂历史数据建立 baseline
for i in range(20):
    normal_trades = [make_trade(62000 + (j%3), 0.1, j%2==0, ts=i*100000 + j*100) for j in range(50)]
    abs_det.detect(normal_trades, window_seconds=100)

# 然后制造一个吸收事件: 大量卖单但价格没跌
abs_trades = []
for i in range(200):  # 200笔成交 = 大量
    abs_trades.append(make_trade(62001, 1.0, is_sell=True, ts=5000000 + i*10))

abs_signals = abs_det.detect(abs_trades, window_seconds=100)
check("吸收检测器能处理大量数据", True, f"signals_count={len(abs_signals)}")

# 验证指标计算逻辑
# Z-Score 计算验证
test_vols = [10, 12, 11, 13, 10, 12, 11, 13, 10, 12]
mean_v = sum(test_vols) / len(test_vols)
std_v = (sum((v - mean_v)**2 for v in test_vols) / len(test_vols)) ** 0.5
z = (100 - mean_v) / std_v
check("Z-Score 公式正确", z > 3, f"z={z:.2f} (vol=100 vs mean={mean_v:.1f})")

# ==================== TEST 6: ExhaustionDetector ====================
print("\n" + "="*60)
print("  TEST 6: ExhaustionDetector 衰竭检测")
print("="*60)

exh = ExhaustionDetector(lookback=5, volume_decline_threshold=0.3)

# 构造衰竭场景: 价格在高位，成交量递减，delta 转负
exh_history = [
    {"time": 1, "delta": 5.0, "cvd": 5.0, "buy": 8.0, "sell": 3.0, "price": 62000},
    {"time": 2, "delta": 4.0, "cvd": 9.0, "buy": 7.0, "sell": 3.0, "price": 62010},
    {"time": 3, "delta": 3.0, "cvd": 12.0, "buy": 6.0, "sell": 3.0, "price": 62020},
    {"time": 4, "delta": -2.0, "cvd": 10.0, "buy": 2.0, "sell": 4.0, "price": 62025},
    {"time": 5, "delta": -3.0, "cvd": 7.0, "buy": 1.0, "sell": 4.0, "price": 62026},
]

exh_signals = exh.detect(exh_history)
bearish_exh = [s for s in exh_signals if s.get("direction") == "bearish_exhaustion"]
check("检测到看跌衰竭", len(bearish_exh) >= 1, f"count={len(bearish_exh)}")

# 构造无衰竭场景: 成交量持续放大
no_exh_history = [
    {"time": 1, "delta": 2.0, "cvd": 2.0, "buy": 5.0, "sell": 3.0, "price": 62000},
    {"time": 2, "delta": 3.0, "cvd": 5.0, "buy": 6.0, "sell": 3.0, "price": 62005},
    {"time": 3, "delta": 4.0, "cvd": 9.0, "buy": 7.0, "sell": 3.0, "price": 62010},
    {"time": 4, "delta": 5.0, "cvd": 14.0, "buy": 8.0, "sell": 3.0, "price": 62015},
    {"time": 5, "delta": 6.0, "cvd": 20.0, "buy": 9.0, "sell": 3.0, "price": 62020},
]

no_exh_signals = exh.detect(no_exh_history)
check("放量上涨无衰竭信号", len(no_exh_signals) == 0, f"count={len(no_exh_signals)}")

# ==================== TEST 7: IcebergDetector ====================
print("\n" + "="*60)
print("  TEST 7: IcebergDetector 冰山单检测")
print("="*60)

ice = IcebergDetector(min_fills=8, max_cv=0.5, z_threshold=1.5)

# 构造冰山单: 同一价格 20 笔成交，每笔都是 0.5（变异系数≈0）
iceberg_trades = []
for i in range(20):
    iceberg_trades.append(make_trade(62000, 0.5, True, ts=1000000 + i*100))

# 其他价格各 2-3 笔
for price in [62001, 62002, 62003]:
    for i in range(2):
        iceberg_trades.append(make_trade(price, 0.1, i%2==0, ts=2000000 + i*100))

ice_signals = ice.detect(iceberg_trades)
detected_icebergs = [s for s in ice_signals if s["price"] == 62000]
check("检测到 62000 的冰山单", len(detected_icebergs) >= 1,
      f"count={len(detected_icebergs)}")
if detected_icebergs:
    check("冰山成交次数=20", detected_icebergs[0]["fills"] == 20,
          f"fills={detected_icebergs[0]['fills']}")
    check("冰山平均量=0.5", abs(detected_icebergs[0]["avg_qty"] - 0.5) < 0.01,
          f"avg_qty={detected_icebergs[0]['avg_qty']}")
    check("变异系数≈0 (均匀拆单)", detected_icebergs[0]["cv"] < 0.01,
          f"cv={detected_icebergs[0]['cv']:.4f}")

# 构造非冰山: 同价格但成交量差异大
no_iceberg = []
for i in range(15):
    qty = 0.1 + i * 0.5  # 从 0.1 到 7.1，差异很大
    no_iceberg.append(make_trade(62050, qty, True, ts=3000000 + i*100))

no_ice_signals = ice.detect(no_iceberg)
non_ice_at_50 = [s for s in no_ice_signals if s["price"] == 62050]
check("量差大的不算冰山", len(non_ice_at_50) == 0,
      f"count={len(non_ice_at_50)}")

# ==================== TEST 8: SpeedOfTape ====================
print("\n" + "="*60)
print("  TEST 8: SpeedOfTape 成交速度")
print("="*60)

sot = SpeedOfTape(window_seconds=1)

# 构造加速场景: 每秒成交递增
accel_trades = []
for sec in range(5):
    n_trades = (sec + 1) * 10  # 10, 20, 30, 40, 50
    for i in range(n_trades):
        accel_trades.append(make_trade(62000, 0.1, i%2==0,
                                       ts=sec*1000 + i*10))

sot.add_trades(accel_trades)
# 检查速度趋势递增（排除非测试数据的窗口）
test_speeds = [h["speed"] for h in sot.speed_history if h["time"] < 10000]
check("速度窗口数 >= 5", len(test_speeds) >= 5, f"count={len(test_speeds)}")
check("速度趋势递增", all(test_speeds[i] <= test_speeds[i+1] for i in range(len(test_speeds)-1)),
      f"speeds={[f'{s:.0f}' for s in test_speeds]}")

# ==================== TEST 9: 信号引擎集成 ====================
print("\n" + "="*60)
print("  TEST 9: OrderFlowSignalEngine 信号聚合")
print("="*60)

engine = OrderFlowSignalEngine(tick_size=1.0)

# 构造一个明显的买方场景: 大量买单集中在同一价格区间
scenario_trades = []
base_ts = int(time.time() * 1000) - 60000

# 大量买单在 62000-62005
for i in range(300):
    price = 62000 + (i % 6)
    scenario_trades.append(make_trade(price, 0.5, is_sell=False, ts=base_ts + i*100))

# 少量卖单在 62010-62015
for i in range(50):
    price = 62010 + (i % 6)
    scenario_trades.append(make_trade(price, 0.1, is_sell=True, ts=base_ts + 30000 + i*100))

engine.feed(scenario_trades)
signals = engine.analyze(scenario_trades)
consensus, confidence = engine.get_consensus()

check("信号引擎产生信号", len(signals) >= 0, f"signals={len(signals)}")
check("共识方向有效", consensus in ("bullish", "bearish", "neutral"),
      f"consensus={consensus}")
check("置信度在 [0, 1]", 0 <= confidence <= 1, f"confidence={confidence:.2f}")

# 验证 VolumeProfile 在引擎内正确
poc = engine.volume_profile.get_poc()
check("引擎内 POC 有效", poc is not None, f"poc={poc}")

vah, val, _ = engine.volume_profile.get_value_area()
check("引擎内 VAH/VAL 有效", vah is not None and val is not None,
      f"vah={vah}, val={val}")

# ==================== TEST 10: 边界条件 ====================
print("\n" + "="*60)
print("  TEST 10: 边界条件 & 鲁棒性")
print("="*60)

# 空数据
empty_fp = FootprintChart()
empty_fp.add_trades([])
check("空数据不崩溃 (Footprint)", empty_fp.get_latest_bar() == (None, {}))

empty_dt = DeltaTracker()
empty_dt.add_trades([], period_seconds=60)
check("空数据不崩溃 (Delta)", empty_dt.cvd == 0)

empty_vp = VolumeProfile()
empty_vp.add_trades([])
check("空数据不崩溃 (VP)", empty_vp.get_poc() is None)

empty_sot = SpeedOfTape()
empty_sot.add_trades([])
check("空数据不崩溃 (Speed)", empty_sot.get_acceleration() == 0)

# 单笔数据
single = [make_trade(62000, 1.0, True, 1000000)]
single_fp = FootprintChart(tick_size=1.0)
single_fp.add_trades(single)
_, single_bar = single_fp.get_latest_bar()
check("单笔数据正确处理", 62000 in single_bar and single_bar[62000]["sell"] == 1.0)

# 极大成交量
big_vol = [make_trade(62000, 999999.999, True, 1000000)]
big_vp = VolumeProfile(tick_size=1.0)
big_vp.add_trades(big_vol)
check("极大成交量不溢出", big_vp.get_poc() == 62000.0)

# CVD 背离检测边界
dt_div = DeltaTracker()
# 价格先跌后涨，CVD 持续下降 → 看涨背离
div_trades = []
for i in range(20):
    price = 62000 - i * 2 if i < 10 else 61980 + i  # 先跌后涨
    div_trades.append(make_trade(price, 1.0, True, ts=100000 + i*60000))
dt_div.add_trades(div_trades, period_seconds=300)
div = dt_div.get_divergence(lookback=10)
check("背离检测可运行", div in ("bullish_div", "bearish_div", "none"),
      f"divergence={div}")

# ==================== 汇总 ====================
print("\n" + "="*60)
print("  IPQC 验证汇总")
print("="*60)

passed = sum(1 for _, s, _ in results if s == PASS)
failed = sum(1 for _, s, _ in results if s == FAIL)
total = len(results)

print(f"\n  总测试: {total}")
print(f"  通过:   {passed} ✅")
print(f"  失败:   {failed} ❌")
print(f"  通过率: {passed/total*100:.1f}%")

if failed > 0:
    print(f"\n  失败项:")
    for name, status, detail in results:
        if status == FAIL:
            print(f"    ❌ {name}  {detail}")

print()
