#!/usr/bin/env python3
"""
币安模拟盘交易机器人
====================
通过 EUserv SOCKS5 代理访问币安 API，支持：
- 现货测试网 / 现货模拟盘(Demo) / 合约测试网
- 市价单、限价单
- 查询余额、持仓、价格

用法:
    python trader.py balance              # 查余额
    python trader.py price BTCUSDT        # 查价格
    python trader.py buy BTCUSDT 0.001    # 市价买入
    python trader.py sell BTCUSDT 0.001   # 市价卖出
    python trader.py limit-buy BTCUSDT 0.001 50000  # 限价买入
    python trader.py limit-sell BTCUSDT 0.001 70000 # 限价卖出
    python trader.py orders BTCUSDT       # 查挂单
    python trader.py history BTCUSDT 20   # 历史订单
    python trader.py trades BTCUSDT 20    # 成交记录
    python trader.py cancel BTCUSDT orderId  # 撤单
    python trader.py positions            # 合约持仓
    python trader.py klines BTCUSDT 1h 10 # K线数据
    python trader.py mode spot_demo       # 切换模式
"""

import sys
import os
import json
import requests
from urllib.parse import urlencode
import hmac
import hashlib
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ==================== 代理请求 ====================

def get_proxies():
    """返回 SOCKS5 代理配置"""
    try:
        from config import PROXY_ENABLED, SOCKS5_PROXY
        if PROXY_ENABLED and SOCKS5_PROXY:
            return {"http": SOCKS5_PROXY, "https": SOCKS5_PROXY}
    except ImportError:
        pass
    return None

def api_request(method, url, params=None, headers=None, signed=False, mode=None):
    """发送 API 请求，自动处理签名和代理"""
    if mode is None:
        mode = get_current_mode()
    
    headers = headers or {}
    
    # 签名请求需要密钥
    if signed:
        from config import (
            SPOT_TESTNET_API_KEY, SPOT_TESTNET_API_SECRET,
            SPOT_DEMO_API_KEY, SPOT_DEMO_API_SECRET,
            FUTURES_DEMO_API_KEY, FUTURES_DEMO_API_SECRET,
        )
        keys = {
            "spot_testnet": (SPOT_TESTNET_API_KEY, SPOT_TESTNET_API_SECRET),
            "spot_demo": (SPOT_DEMO_API_KEY, SPOT_DEMO_API_SECRET),
            "futures_demo": (FUTURES_DEMO_API_KEY, FUTURES_DEMO_API_SECRET),
        }
        api_key, api_secret = keys.get(mode, ("", ""))
        if not api_key:
            print(f"❌ 未配置 {mode} 的 API Key，请编辑 config.py")
            sys.exit(1)
        headers["X-MBX-APIKEY"] = api_key
    
    if params is None:
        params = {}
    
    # 签名
    if signed:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        query_string = urlencode(params)
        signature = hmac.new(
            api_secret.encode(), query_string.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
    
    proxies = get_proxies()
    
    if method == "GET":
        resp = requests.get(url, params=params, headers=headers, proxies=proxies, timeout=15)
    elif method == "POST":
        resp = requests.post(url, params=params, headers=headers, proxies=proxies, timeout=15)
    elif method == "DELETE":
        resp = requests.delete(url, params=params, headers=headers, proxies=proxies, timeout=15)
    else:
        raise ValueError(f"Unsupported method: {method}")
    
    if resp.status_code != 200:
        print(f"❌ API 错误 [{resp.status_code}]: {resp.text}")
        return None
    
    return resp.json()

# ==================== 模式切换 ====================

MODE_FILE = os.path.join(BASE_DIR, ".current_mode")

def get_current_mode():
    try:
        with open(MODE_FILE) as f:
            return f.read().strip()
    except FileNotFoundError:
        return "spot_testnet"

def set_mode(mode):
    valid = ["spot_testnet", "spot_demo", "futures_demo"]
    if mode not in valid:
        print(f"❌ 无效模式: {mode}，可选: {', '.join(valid)}")
        return
    with open(MODE_FILE, "w") as f:
        f.write(mode)
    print(f"✅ 已切换到: {mode}")

def get_base_url(mode=None):
    from config import SPOT_TESTNET_BASE, SPOT_DEMO_BASE, FUTURES_DEMO_BASE
    if mode is None:
        mode = get_current_mode()
    urls = {
        "spot_testnet": SPOT_TESTNET_BASE,
        "spot_demo": SPOT_DEMO_BASE,
        "futures_demo": FUTURES_DEMO_BASE,
    }
    return urls[mode]

# ==================== 交易功能 ====================

def get_price(symbol):
    """获取最新价格"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    if mode.startswith("futures"):
        url = f"{base}/fapi/v1/ticker/price"
    else:
        url = f"{base}/api/v3/ticker/price"
    
    data = api_request("GET", url, {"symbol": symbol}, mode=mode)
    if data:
        price = float(data["price"])
        print(f"💰 {symbol} 当前价格: {price:,.2f} USDT")
        return price
    return None

def get_balance():
    """查询账户余额"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    if mode.startswith("futures"):
        url = f"{base}/fapi/v2/balance"
        data = api_request("GET", url, signed=True, mode=mode)
        if data:
            print("📊 合约账户余额:")
            for item in data:
                bal = float(item["balance"])
                if bal != 0:
                    print(f"  {item['asset']}: 余额={bal}, 可用={float(item['availableBalance'])}")
    else:
        url = f"{base}/api/v3/account"
        data = api_request("GET", url, signed=True, mode=mode)
        if data:
            print("📊 现货账户余额:")
            for item in data["balances"]:
                free = float(item["free"])
                locked = float(item["locked"])
                if free > 0 or locked > 0:
                    print(f"  {item['asset']}: 可用={free}, 冻结={locked}")

def place_order(symbol, side, order_type, quantity, price=None):
    """下单"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    if mode.startswith("futures"):
        url = f"{base}/fapi/v1/order"
    else:
        url = f"{base}/api/v3/order"
    
    params = {
        "symbol": symbol,
        "side": side,  # BUY / SELL
        "type": order_type,  # MARKET / LIMIT
        "quantity": quantity,
    }
    
    if order_type == "LIMIT":
        if price is None:
            print("❌ 限价单必须指定价格")
            return None
        params["price"] = price
        params["timeInForce"] = "GTC"
    
    action = "买入" if side == "BUY" else "卖出"
    print(f"📤 下单: {action} {quantity} {symbol} @ {'市价' if order_type == 'MARKET' else price}")
    
    data = api_request("POST", url, params, signed=True, mode=mode)
    if data:
        print(f"✅ 订单成功! ID: {data.get('orderId', 'N/A')}")
        print(f"   状态: {data.get('status', 'N/A')}")
        if 'fills' in data:
            for fill in data['fills']:
                print(f"   成交: {fill['qty']} @ {fill['price']}")
        return data
    return None

def get_open_orders(symbol=None):
    """查询挂单"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    if mode.startswith("futures"):
        url = f"{base}/fapi/v1/openOrders"
    else:
        url = f"{base}/api/v3/openOrders"
    
    params = {}
    if symbol:
        params["symbol"] = symbol
    
    data = api_request("GET", url, params, signed=True, mode=mode)
    if data:
        if not data:
            print("📭 没有挂单")
            return data
        print(f"📋 挂单 ({len(data)} 个):")
        for o in data:
            print(f"  [{o['orderId']}] {o['side']} {o['origQty']} {o['symbol']} @ {o.get('price', 'MARKET')} [{o['status']}]")
        return data
    return None

def cancel_order(symbol, order_id):
    """撤单"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    if mode.startswith("futures"):
        url = f"{base}/fapi/v1/order"
    else:
        url = f"{base}/api/v3/order"
    
    params = {"symbol": symbol, "orderId": order_id}
    data = api_request("DELETE", url, params, signed=True, mode=mode)
    if data:
        print(f"✅ 已撤单: {order_id}")
        return data
    return None

def place_stop_order(symbol, side, stop_price, quantity=None):
    """
    下止损单（服务端执行，程序崩溃也能触发）
    
    Args:
        symbol: 交易对
        side: "SELL" (平多) 或 "BUY" (平空)
        stop_price: 触发价格
        quantity: 平仓数量（不传则 closePosition=true 全平）
    """
    mode = get_current_mode()
    base = get_base_url(mode)
    url = f"{base}/fapi/v1/order"
    
    params = {
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "stopPrice": round(stop_price, 1),
        "workingType": "MARK_PRICE",
    }
    
    if quantity:
        params["quantity"] = quantity
    else:
        params["closePosition"] = "true"
    
    data = api_request("POST", url, params, signed=True, mode=mode)
    if data:
        print(f"✅ 止损单已下: {side} @ {stop_price:.1f} (ID: {data.get('orderId', 'N/A')})")
        return data
    return None

def place_take_profit_order(symbol, side, stop_price, quantity=None):
    """
    下止盈单（服务端执行）
    
    Args:
        symbol: 交易对
        side: "SELL" (平多) 或 "BUY" (平空)
        stop_price: 触发价格
        quantity: 平仓数量（不传则 closePosition=true 全平）
    """
    mode = get_current_mode()
    base = get_base_url(mode)
    url = f"{base}/fapi/v1/order"
    
    params = {
        "symbol": symbol,
        "side": side,
        "type": "TAKE_PROFIT_MARKET",
        "stopPrice": round(stop_price, 1),
        "workingType": "MARK_PRICE",
    }
    
    if quantity:
        params["quantity"] = quantity
    else:
        params["closePosition"] = "true"
    
    data = api_request("POST", url, params, signed=True, mode=mode)
    if data:
        print(f"✅ 止盈单已下: {side} @ {stop_price:.1f} (ID: {data.get('orderId', 'N/A')})")
        return data
    return None

def cancel_all_orders(symbol):
    """撤销指定交易对的所有挂单"""
    mode = get_current_mode()
    base = get_base_url(mode)
    url = f"{base}/fapi/v1/allOpenOrders"
    params = {"symbol": symbol}
    data = api_request("DELETE", url, params, signed=True, mode=mode)
    if data:
        print(f"✅ 已撤销 {symbol} 所有挂单")
        return data
    return None

def get_all_orders(symbol="BTCUSDT", limit=20):
    """
    查询历史订单（含已成交、已取消、过期）

    Args:
        symbol: 交易对
        limit: 返回条数（默认20，最大1000）

    Returns:
        list of order dicts, or None
    """
    mode = get_current_mode()
    base = get_base_url(mode)

    if mode.startswith("futures"):
        url = f"{base}/fapi/v1/allOrders"
    else:
        url = f"{base}/api/v3/allOrders"

    params = {"symbol": symbol, "limit": limit}
    data = api_request("GET", url, params, signed=True, mode=mode)
    if data:
        if not data:
            print("📭 没有历史订单")
            return data
        print(f"📋 历史订单 ({len(data)} 个):")
        for o in data:
            status = o.get("status", "?")
            side = o.get("side", "?")
            qty = o.get("origQty", "?")
            price = o.get("price", "MARKET")
            avg_price = o.get("avgPrice", "0")
            ts = o.get("time", 0)
            time_str = time.strftime("%m-%d %H:%M", time.localtime(ts / 1000)) if ts else "?"
            icon = {"FILLED": "✅", "CANCELED": "❌", "EXPIRED": "⏰", "NEW": "🔵"}.get(status, "❓")
            avg_info = f" avg={avg_price}" if float(avg_price or 0) > 0 else ""
            print(f"  {icon} [{o['orderId']}] {time_str} {side} {qty} {symbol} @ {price}{avg_info} [{status}]")
        return data
    return None

def get_my_trades(symbol="BTCUSDT", limit=20):
    """
    查询成交记录（实际撮合的交易）

    Args:
        symbol: 交易对
        limit: 返回条数（默认20，最大1000）

    Returns:
        list of trade dicts, or None
    """
    mode = get_current_mode()
    base = get_base_url(mode)

    if mode.startswith("futures"):
        url = f"{base}/fapi/v1/userTrades"
    else:
        url = f"{base}/api/v3/myTrades"

    params = {"symbol": symbol, "limit": limit}
    data = api_request("GET", url, params, signed=True, mode=mode)
    if data:
        if not data:
            print("📭 没有成交记录")
            return data
        print(f"📊 成交记录 ({len(data)} 笔):")
        total_pnl = 0.0
        total_fee = 0.0
        for t in data:
            price = float(t.get("price", 0))
            qty = float(t.get("qty", 0))
            quote_qty = float(t.get("quoteQty", 0))
            commission = float(t.get("commission", 0))
            pnl = float(t.get("realizedPnl", 0))
            side = t.get("side", "?")
            ts = t.get("time", 0)
            time_str = time.strftime("%m-%d %H:%M:%S", time.localtime(ts / 1000)) if ts else "?"
            maker = "🟡Maker" if t.get("maker", False) else "🔵Taker"
            pnl_str = f" PnL={pnl:+.4f}" if pnl != 0 else ""
            print(f"  {time_str} {side} {qty} @ {price:.1f} ({quote_qty:.2f} USDT) fee={commission:.6f}{pnl_str} {maker}")
            total_pnl += pnl
            total_fee += commission
        print(f"\n  合计: PnL={total_pnl:+.4f} USDT, 手续费={total_fee:.6f} USDT")
        return data
    return None

def get_positions():
    """查询合约持仓（仅合约模式）"""
    mode = get_current_mode()
    if not mode.startswith("futures"):
        print("⚠️ 持仓查询仅支持合约模式，请先切换: python trader.py mode futures_demo")
        return None

    base = get_base_url(mode)
    url = f"{base}/fapi/v2/positionRisk"
    data = api_request("GET", url, signed=True, mode=mode)
    if data:
        active = [p for p in data if float(p["positionAmt"]) != 0]
        if not active:
            print("📭 没有持仓")
            return data
        print(f"📊 当前持仓 ({len(active)} 个):")
        for p in active:
            amt = float(p["positionAmt"])
            entry = float(p["entryPrice"])
            pnl = float(p["unRealizedProfit"])
            side = "多" if amt > 0 else "空"
            print(f"  {p['symbol']}: {side} {abs(amt)} @ {entry}, 浮盈: {pnl:+.4f} USDT")
        return data
    return None

def is_position_active(symbol="BTCUSDT"):
    """
    轻量级检查指定交易对是否有活跃持仓

    Returns:
        True if position exists with non-zero amount, False otherwise
    """
    mode = get_current_mode()
    if not mode.startswith("futures"):
        return False
    base = get_base_url(mode)
    url = f"{base}/fapi/v2/positionRisk"
    data = api_request("GET", url, signed=True, mode=mode)
    if data:
        for p in data:
            if p.get("symbol") == symbol and float(p.get("positionAmt", 0)) != 0:
                return True
    return False

def get_klines(symbol, interval="1h", limit=10):
    """获取 K 线数据"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    if mode.startswith("futures"):
        url = f"{base}/fapi/v1/klines"
    else:
        url = f"{base}/api/v3/klines"
    
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    data = api_request("GET", url, params, mode=mode)
    if data:
        print(f"📈 {symbol} K线 ({interval}, 最近{limit}根):")
        print(f"  {'时间':>16} {'开':>12} {'高':>12} {'低':>12} {'收':>12} {'量':>12}")
        for k in data:
            t = time.strftime("%m-%d %H:%M", time.localtime(k[0] / 1000))
            print(f"  {t:>16} {float(k[1]):>12,.2f} {float(k[2]):>12,.2f} {float(k[3]):>12,.2f} {float(k[4]):>12,.2f} {float(k[5]):>12,.4f}")
        return data
    return None

def test_connection():
    """测试连接"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    print(f"🔍 测试连接: {mode}")
    print(f"   端点: {base}")
    
    # 公开 API 测试（不需要签名）
    if mode.startswith("futures"):
        url = f"{base}/fapi/v1/ping"
    else:
        url = f"{base}/api/v3/ping"
    
    data = api_request("GET", url, mode=mode)
    if data is not None:
        print("✅ API 连通!")
        
        # 测试价格
        if mode.startswith("futures"):
            url = f"{base}/fapi/v1/ticker/price"
        else:
            url = f"{base}/api/v3/ticker/price"
        
        price_data = api_request("GET", url, {"symbol": "BTCUSDT"}, mode=mode)
        if price_data:
            print(f"💰 BTC 价格: {float(price_data['price']):,.2f} USDT")
    else:
        print("❌ 连接失败!")

# ==================== 行情数据 ====================

def get_oi(symbol="BTCUSDT", period="1h", limit=10):
    """持仓量 (Open Interest)"""
    mode = get_current_mode()
    base = get_base_url(mode)
    real = "https://fapi.binance.com"
    
    # 当前 OI（demo 有数据）
    r = api_request("GET", f"{base}/fapi/v1/openInterest", {"symbol": symbol}, mode=mode)
    if r:
        current_oi = float(r["openInterest"])
        print(f"📊 {symbol} 当前持仓量: {current_oi:,.4f} BTC")
    
    # OI 历史（demo 无数据，走真实 API）
    r = api_request("GET", f"{real}/futures/data/openInterestHist",
                    {"symbol": symbol, "period": period, "limit": limit}, mode=mode)
    if r and isinstance(r, list) and len(r) > 0 and isinstance(r[0], dict) and "sumOpenInterest" in r[0]:
        print(f"\n📈 持仓量变化 ({period}, 最近{limit}期):")
        for item in r:
            t = time.strftime("%m-%d %H:%M", time.localtime(item["timestamp"] / 1000))
            oi = float(item["sumOpenInterest"])
            oi_val = float(item["sumOpenInterestValue"])
            print(f"  {t}  OI: {oi:>14,.2f} BTC  价值: ${oi_val/1e9:>8,.2f}B")

def get_cvd(symbol="BTCUSDT", limit=1000):
    """CVD (Cumulative Volume Delta) - 从聚合成交计算"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    r = api_request("GET", f"{base}/fapi/v1/aggTrades",
                    {"symbol": symbol, "limit": limit}, mode=mode)
    if not r:
        return
    
    buy_vol = 0.0
    sell_vol = 0.0
    for trade in r:
        qty = float(trade["q"])
        if trade["m"]:  # isBuyerMaker=true → 卖方主动成交
            sell_vol += qty
        else:           # isBuyerMaker=false → 买方主动成交
            buy_vol += qty
    
    cvd = buy_vol - sell_vol
    total = buy_vol + sell_vol
    
    print(f"📊 {symbol} CVD (最近{len(r)}笔成交):")
    print(f"  主买量: {buy_vol:>12,.4f} BTC  ({buy_vol/total*100:.1f}%)")
    print(f"  主卖量: {sell_vol:>12,.4f} BTC  ({sell_vol/total*100:.1f}%)")
    print(f"  CVD:    {cvd:>+12,.4f} BTC  {'🟢 买方主导' if cvd > 0 else '🔴 卖方主导'}")

def get_vol(symbol="BTCUSDT", interval="1h", limit=24):
    """成交量 (Volume)"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    r = api_request("GET", f"{base}/fapi/v1/klines",
                    {"symbol": symbol, "interval": interval, "limit": limit}, mode=mode)
    if not r:
        return
    
    print(f"📊 {symbol} 成交量 ({interval}, 最近{limit}根):")
    total_vol = 0
    total_buy_vol = 0
    for k in r:
        t = time.strftime("%m-%d %H:%M", time.localtime(k[0] / 1000))
        vol = float(k[5])         # 总成交量
        buy_vol = float(k[9])     # 主动买入量
        sell_vol = vol - buy_vol
        total_vol += vol
        total_buy_vol += buy_vol
        bar = "█" * max(1, int(vol / max(float(k2[5]) for k2 in r) * 20))
        cvd_color = "+" if buy_vol > sell_vol else "-"
        print(f"  {t}  {vol:>12,.2f} {bar}  主买比: {buy_vol/vol*100:.0f}%")
    
    print(f"\n  合计: {total_vol:,.2f} BTC, 主买占比: {total_buy_vol/total_vol*100:.1f}%")

def get_long_short_ratio(symbol="BTCUSDT", period="1h", limit=10):
    """多空比"""
    mode = get_current_mode()
    real = "https://fapi.binance.com"
    
    # 全账户多空比（demo 无数据，走真实 API）
    r = api_request("GET", f"{real}/futures/data/globalLongShortAccountRatio",
                    {"symbol": symbol, "period": period, "limit": limit}, mode=mode)
    if r and isinstance(r, list) and len(r) > 0 and isinstance(r[0], dict) and "longAccount" in r[0]:
        print(f"📊 {symbol} 全账户多空比 ({period}):")
        for item in r:
            t = time.strftime("%m-%d %H:%M", time.localtime(item["timestamp"] / 1000))
            long_r = float(item["longAccount"]) * 100
            short_r = float(item["shortAccount"]) * 100
            ratio = float(item["longShortRatio"])
            bar = "🟢" * min(20, int(long_r / 5)) + "🔴" * min(20, int(short_r / 5))
            print(f"  {t}  多: {long_r:.1f}%  空: {short_r:.1f}%  比值: {ratio:.3f}  {bar}")
    
    # 大户持仓比
    r = api_request("GET", f"{real}/futures/data/topLongShortPositionRatio",
                    {"symbol": symbol, "period": period, "limit": limit}, mode=mode)
    if r and isinstance(r, list) and len(r) > 0 and isinstance(r[0], dict) and "longAccount" in r[0]:
        print(f"\n📊 {symbol} 大户持仓多空比 ({period}):")
        for item in r:
            t = time.strftime("%m-%d %H:%M", time.localtime(item["timestamp"] / 1000))
            long_r = float(item["longAccount"]) * 100
            short_r = float(item["shortAccount"]) * 100
            ratio = float(item["longShortRatio"])
            print(f"  {t}  多: {long_r:.1f}%  空: {short_r:.1f}%  比值: {ratio:.3f}")
    
    # Taker 买卖比
    r = api_request("GET", f"{real}/futures/data/takerlongshortRatio",
                    {"symbol": symbol, "period": period, "limit": limit}, mode=mode)
    if r and isinstance(r, list) and len(r) > 0 and isinstance(r[0], dict) and "buyVol" in r[0]:
        print(f"\n📊 {symbol} Taker 主动买卖比 ({period}):")
        for item in r:
            t = time.strftime("%m-%d %H:%M", time.localtime(item["timestamp"] / 1000))
            buy_vol = float(item["buySellRatio"])
            buy_v = float(item["buyVol"])
            sell_v = float(item["sellVol"])
            print(f"  {t}  买/卖比: {buy_vol:.3f}  买量: {buy_v:,.2f}  卖量: {sell_v:,.2f}")

def get_funding(symbol="BTCUSDT", limit=5):
    """资金费率"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    r = api_request("GET", f"{base}/fapi/v1/fundingRate",
                    {"symbol": symbol, "limit": limit}, mode=mode)
    if r:
        print(f"📊 {symbol} 资金费率:")
        for item in r:
            t = time.strftime("%m-%d %H:%M", time.localtime(item["fundingTime"] / 1000))
            rate = float(item["fundingRate"]) * 100
            mark = float(item.get("markPrice", 0))
            indicator = "🟢" if rate > 0 else "🔴"
            print(f"  {t}  费率: {rate:+.4f}% {indicator}  标记价: {mark:,.2f}")
    
    # Mark Price & Basis
    r = api_request("GET", f"{base}/fapi/v1/premiumIndex", {"symbol": symbol}, mode=mode)
    if r:
        mark = float(r["markPrice"])
        index = float(r["indexPrice"])
        basis = (mark - index) / index * 100
        next_funding = time.strftime("%m-%d %H:%M", time.localtime(r["nextFundingTime"] / 1000))
        print(f"\n  标记价: {mark:,.2f}  指数: {index:,.2f}  基差: {basis:+.4f}%")
        print(f"  下次资金费: {next_funding}  费率: {float(r['lastFundingRate'])*100:+.4f}%")

def get_ticker(symbol="BTCUSDT"):
    """24h 行情摘要"""
    mode = get_current_mode()
    base = get_base_url(mode)
    
    r = api_request("GET", f"{base}/fapi/v1/ticker/24hr", {"symbol": symbol}, mode=mode)
    if r:
        print(f"📊 {symbol} 24h 行情:")
        print(f"  最新价:   {float(r['lastPrice']):>12,.2f}")
        print(f"  24h涨跌:  {float(r['priceChange']):>+12,.2f} ({float(r['priceChangePercent']):+.2f}%)")
        print(f"  最高价:   {float(r['highPrice']):>12,.2f}")
        print(f"  最低价:   {float(r['lowPrice']):>12,.2f}")
        print(f"  成交量:   {float(r['volume']):>12,.2f} BTC")
        print(f"  成交额:   {float(r['quoteVolume']):>16,.2f} USDT")

def get_market_overview(symbol="BTCUSDT"):
    """综合面板 - 一次看全"""
    print(f"{'='*50}")
    print(f"  {symbol} 市场综合面板")
    print(f"{'='*50}\n")
    
    get_ticker(symbol)
    print()
    get_cvd(symbol, limit=500)
    print()
    get_oi(symbol, period="1h", limit=5)
    print()
    get_funding(symbol, limit=3)
    print()
    get_long_short_ratio(symbol, period="1h", limit=5)

# ==================== 主入口 ====================

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    
    cmd = sys.argv[1].lower()
    
    if cmd == "mode":
        if len(sys.argv) < 3:
            print(f"当前模式: {get_current_mode()}")
            print("可选: spot_testnet, spot_demo, futures_demo")
        else:
            set_mode(sys.argv[2])
    
    elif cmd == "test":
        test_connection()
    
    elif cmd == "price":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        get_price(symbol)
    
    elif cmd == "balance":
        get_balance()
    
    elif cmd == "buy":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        qty = sys.argv[3] if len(sys.argv) > 3 else "0.001"
        place_order(symbol, "BUY", "MARKET", qty)
    
    elif cmd == "sell":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        qty = sys.argv[3] if len(sys.argv) > 3 else "0.001"
        place_order(symbol, "SELL", "MARKET", qty)
    
    elif cmd == "limit-buy":
        if len(sys.argv) < 5:
            print("用法: python trader.py limit-buy SYMBOL QTY PRICE")
            return
        place_order(sys.argv[2], "BUY", "LIMIT", sys.argv[3], sys.argv[4])
    
    elif cmd == "limit-sell":
        if len(sys.argv) < 5:
            print("用法: python trader.py limit-sell SYMBOL QTY PRICE")
            return
        place_order(sys.argv[2], "SELL", "LIMIT", sys.argv[3], sys.argv[4])
    
    elif cmd == "orders":
        symbol = sys.argv[2] if len(sys.argv) > 2 else None
        get_open_orders(symbol)
    
    elif cmd == "cancel":
        if len(sys.argv) < 4:
            print("用法: python trader.py cancel SYMBOL ORDER_ID")
            return
        cancel_order(sys.argv[2], sys.argv[3])
    
    elif cmd == "positions":
        get_positions()

    elif cmd == "history" or cmd == "all-orders":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 20
        get_all_orders(symbol, limit)

    elif cmd == "trades" or cmd == "my-trades":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 20
        get_my_trades(symbol, limit)
    
    elif cmd == "klines":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        interval = sys.argv[3] if len(sys.argv) > 3 else "1h"
        limit = int(sys.argv[4]) if len(sys.argv) > 4 else 10
        get_klines(symbol, interval, limit)
    
    # ---- 行情数据 ----
    elif cmd == "oi":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        period = sys.argv[3] if len(sys.argv) > 3 else "1h"
        get_oi(symbol, period)
    
    elif cmd == "cvd":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
        get_cvd(symbol, limit)
    
    elif cmd == "vol":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        interval = sys.argv[3] if len(sys.argv) > 3 else "1h"
        limit = int(sys.argv[4]) if len(sys.argv) > 4 else 24
        get_vol(symbol, interval, limit)
    
    elif cmd == "ratio":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        period = sys.argv[3] if len(sys.argv) > 3 else "1h"
        get_long_short_ratio(symbol, period)
    
    elif cmd == "funding":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        get_funding(symbol)
    
    elif cmd == "ticker":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        get_ticker(symbol)
    
    elif cmd == "overview" or cmd == "ov":
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        get_market_overview(symbol)
    
    elif cmd == "flow" or cmd == "of":
        # 订单流分析
        from orderflow import run_analysis
        symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
        minutes = int(sys.argv[3]) if len(sys.argv) > 3 else 30
        run_analysis(symbol, minutes)
    
    elif cmd == "help":
        print(__doc__)
    
    else:
        print(f"❌ 未知命令: {cmd}")
        print("运行 python trader.py help 查看帮助")

if __name__ == "__main__":
    main()
