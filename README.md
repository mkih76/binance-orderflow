# 📊 Binance Order Flow Trading System

基于 ATAS 核心功能的币安合约订单流交易系统，通过 EUserv SOCKS5 代理访问币安 API。

## 功能

### 订单流分析引擎 (`orderflow.py`)

| 模块 | 功能 | 状态 |
|------|------|------|
| **Footprint Chart** | 每个价格的买×卖量（足迹图） | ✅ |
| **Delta / CVD** | 主动买卖差 + 累积量差 + 背离检测 | ✅ |
| **Volume Profile** | POC / VAH / VAL / HVN / LVN | ✅ |
| **Stacked Imbalance** | 堆叠失衡（3:1 连续 3 层） | ✅ |
| **Absorption** | 吸收检测（Z-Score + 失衡 + 低价格影响） | ✅ |
| **Exhaustion** | 衰竭检测（量缩 + 价格极端 + Delta 背离） | ✅ |
| **Iceberg Detection** | 冰山单检测（重复成交 + 均匀拆单） | ✅ |
| **Speed of Tape** | 成交速度 / 加速度 / 动量方向 | ✅ |
| **Signal Engine** | 多指标加权聚合 → 共识信号 | ✅ |

### 交易系统

| 模块 | 功能 |
|------|------|
| `trader.py` | 币安模拟盘交易（市价/限价/撤单/持仓/行情数据） |
| `strategy.py` | REST 轮询版自动交易策略 |
| `realtime.py` | **WebSocket 实时版**（毫秒级数据流） |
| `test_ipqc.py` | IPQC 验证（45 项测试，100% 通过） |

## 快速开始

### 1. 安装依赖

```bash
pip install python-binance PySocks websocket-client requests
```

### 2. 配置 API Key

```bash
cp config.template.py config.py
# 编辑 config.py，填入你的币安模拟盘 API Key
```

获取 API Key:
- **合约模拟盘**: https://demo.binance.com → 登录 → 合约交易 → 右上角 API 管理

### 3. 设置代理（可选）

如果 VPS IP 被币安封锁（如美国 IP），需要通过代理访问：

```bash
# 通过 SSH 隧道建立 SOCKS5 代理
ssh -D 127.0.0.1:1080 -N user@your-proxy-server
```

在 `config.py` 中设置:
```python
PROXY_ENABLED = True
SOCKS5_PROXY = "socks5://127.0.0.1:1080"
```

### 4. 运行

```bash
# 查看行情数据
python3 trader.py price BTCUSDT
python3 trader.py ov BTCUSDT        # 综合面板
python3 trader.py flow BTCUSDT      # 订单流分析

# 交易
python3 trader.py buy BTCUSDT 0.001 # 市价买入
python3 trader.py balance           # 查看余额
python3 trader.py positions         # 查看持仓

# 实时 WebSocket 系统
python3 realtime.py --dry-run       # 干跑（只看信号不下单）
python3 realtime.py                 # 实盘

# REST 轮询策略
python3 strategy.py --dry-run       # 干跑
python3 strategy.py                 # 实盘

# IPQC 验证
python3 test_ipqc.py                # 运行 45 项测试
```

## 项目结构

```
binance-testnet/
├── README.md               # 本文件
├── config.template.py      # 配置模板（不含密钥）
├── config.py               # 实际配置（.gitignore 排除）
├── TRADING_PLAN.md         # 交易方案文档
├── orderflow.py            # 订单流分析引擎（核心）
├── trader.py               # 币安交易 API 封装
├── strategy.py             # REST 轮询版自动策略
├── realtime.py             # WebSocket 实时版策略
└── test_ipqc.py            # IPQC 验证测试
```

## 架构

```
┌─────────────────────────────────────────────────────┐
│                  VPS (本机)                          │
│                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────────┐    │
│  │ realtime  │──▶│ orderflow │──▶│  trader.py   │    │
│  │  (WS)    │   │  engine   │   │  (下单执行)   │    │
│  └────┬─────┘   └──────────┘   └──────────────┘    │
│       │                                             │
│  ┌────┴─────┐                                       │
│  │ SOCKS5   │                                       │
│  │ :1080    │                                       │
│  └────┬─────┘                                       │
└───────┼─────────────────────────────────────────────┘
        │ SSH 隧道
        ▼
┌───────────────┐        ┌───────────────────┐
│  EUserv       │───────▶│  Binance Futures   │
│  (德国)       │        │  API / WebSocket   │
└───────────────┘        └───────────────────┘
```

## 交易方案

详见 [TRADING_PLAN.md](TRADING_PLAN.md)

**核心参数:**
- 本金: 100 USDT | 杠杆: 3x | 标的: BTCUSDT 永续
- 主周期: 5 分钟 | 入场: 1 分钟
- 每笔风险: 2% | 止损: 0.65% | 止盈: 1:2 R:R
- 入场条件: ≥2 个信号一致（堆叠失衡/吸收/CVD背离/VA边缘）

## IPQC 验证

```
总测试: 45
通过:   45 ✅
失败:   0 ❌
通过率: 100.0%
```

覆盖模块: FootprintChart, DeltaTracker, VolumeProfile, ImbalanceDetector, AbsorptionDetector, ExhaustionDetector, IcebergDetector, SpeedOfTape, SignalEngine, 边界条件

## 免责声明

⚠️ 本项目仅供学习和研究目的。加密货币交易具有高风险，模拟盘结果不代表实盘表现。使用者需自行承担所有交易风险。
