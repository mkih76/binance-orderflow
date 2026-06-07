"""
币安模拟盘交易配置
=================
复制此文件为 config.py 并填入你的 API Key

API Key 获取方式:
- 现货测试网: https://testnet.binance.vision/ (GitHub 登录)
- 现货模拟盘(Demo): 币安官网 → 模拟交易 → API 管理
- 合约模拟盘(Demo): https://demo.binance.com → 合约 → API 管理
"""

# ==================== API 密钥 ====================
# 合约 Demo 密钥（demo.binance.com → API 管理创建）
FUTURES_DEMO_API_KEY = "your_api_key_here"
FUTURES_DEMO_API_SECRET = "your_api_secret_here"

# 现货测试网密钥（testnet.binance.vision 生成的）
SPOT_TESTNET_API_KEY = ""
SPOT_TESTNET_API_SECRET = ""

# 现货模拟盘密钥（币安 Demo 模式 API 管理创建的）
SPOT_DEMO_API_KEY = ""
SPOT_DEMO_API_SECRET = ""

# ==================== 代理配置 ====================
# EUserv SOCKS5 代理（通过 SSH 隧道）
# 如果直连币安，设为 False
PROXY_ENABLED = True
SOCKS5_PROXY = "socks5://127.0.0.1:1080"

# ==================== API 端点 ====================
# 现货测试网
SPOT_TESTNET_BASE = "https://testnet.binance.vision"
SPOT_TESTNET_WS = "wss://stream.testnet.binance.vision/ws"

# 现货模拟盘 (Demo)
SPOT_DEMO_BASE = "https://demo-api.binance.com"
SPOT_DEMO_WS = "wss://demo-stream.binance.com/ws"

# 合约模拟盘 (Demo Trading)
FUTURES_DEMO_BASE = "https://demo-fapi.binance.com"
FUTURES_DEMO_WS = "wss://fstream.binancefuture.com/ws"

# ==================== 默认设置 ====================
DEFAULT_MODE = "futures_demo"  # spot_testnet / spot_demo / futures_demo
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_QUANTITY = 0.001  # BTC 数量
