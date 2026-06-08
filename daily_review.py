#!/usr/bin/env python3
"""
每日交易复盘 + AI 改进建议 + 邮件发送
======================================
用法:
    python daily_review.py              # 生成复盘 + 发邮件
    python daily_review.py --no-email   # 只生成复盘，不发邮件
    python daily_review.py --test-email # 测试邮件发送
"""

import json
import os
import sys
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

BJT = timezone(timedelta(hours=8))

# ==================== 读取交易日志 ====================

def load_journal(journal_file=None):
    if not journal_file:
        journal_file = os.path.join(BASE_DIR, "trade_journal.jsonl")
    records = []
    try:
        with open(journal_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    except FileNotFoundError:
        pass
    return records


def group_trades(records):
    trades = {}
    for r in records:
        tid = r.get("trade_id", "unknown")
        if tid not in trades:
            trades[tid] = {"entry": None, "exit": None, "orders": [], "snapshot": None}
        rtype = r.get("type")
        if rtype == "entry":
            trades[tid]["entry"] = r
        elif rtype == "exit":
            trades[tid]["exit"] = r
        elif rtype == "order":
            trades[tid]["orders"].append(r)
        elif rtype == "snapshot":
            trades[tid]["snapshot"] = r
    return trades


def filter_today(trades):
    """筛选今天的交易"""
    today_str = datetime.now(BJT).strftime("%Y-%m-%d")
    today_trades = {}
    for tid, t in trades.items():
        entry = t.get("entry")
        if entry and entry.get("time_str", "").startswith(today_str):
            today_trades[tid] = t
    return today_trades


def filter_recent(trades, days=7):
    """筛选最近 N 天的交易"""
    cutoff = time.time() - days * 86400
    recent = {}
    for tid, t in trades.items():
        entry = t.get("entry")
        if entry and entry.get("timestamp", 0) >= cutoff:
            recent[tid] = t
    return recent


# ==================== 统计分析 ====================

def compute_stats(trades):
    completed = [(tid, t) for tid, t in trades.items() if t["exit"]]
    if not completed:
        return None

    wins = 0
    losses = 0
    total_pnl = 0.0
    total_pnl_pct = 0.0
    win_pnls = []
    loss_pnls = []
    win_hold = []
    loss_hold = []
    signal_stats = {}
    exit_reasons = {}
    hourly_pnl = {}

    for tid, t in completed:
        entry = t["entry"]
        exit_ = t["exit"]
        signals = entry.get("signals", [])

        pnl = exit_.get("pnl", 0)
        pnl_pct = exit_.get("pnl_pct", 0)
        hold = exit_.get("hold_time_minutes", 0)
        er = exit_.get("exit_reason", "?")

        total_pnl += pnl
        total_pnl_pct += pnl_pct

        if pnl > 0:
            wins += 1
            win_pnls.append(pnl)
            win_hold.append(hold)
        else:
            losses += 1
            loss_pnls.append(pnl)
            loss_hold.append(hold)

        # 信号源胜率
        for s in signals:
            src = s.get("source", "unknown")
            if src not in signal_stats:
                signal_stats[src] = {"wins": 0, "losses": 0, "pnl": 0}
            if pnl > 0:
                signal_stats[src]["wins"] += 1
            else:
                signal_stats[src]["losses"] += 1
            signal_stats[src]["pnl"] += pnl

        # 出场原因
        exit_reasons[er] = exit_reasons.get(er, 0) + 1

        # 按小时统计
        entry_time = entry.get("time_str", "")
        if len(entry_time) >= 13:
            hour = entry_time[11:13]
            if hour not in hourly_pnl:
                hourly_pnl[hour] = {"pnl": 0, "count": 0}
            hourly_pnl[hour]["pnl"] += pnl
            hourly_pnl[hour]["count"] += 1

    total = wins + losses
    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / total if total else 0,
        "total_pnl": total_pnl,
        "total_pnl_pct": total_pnl_pct,
        "avg_win": sum(win_pnls) / len(win_pnls) if win_pnls else 0,
        "avg_loss": sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0,
        "profit_factor": abs(sum(win_pnls) / sum(loss_pnls)) if loss_pnls and sum(loss_pnls) != 0 else 0,
        "avg_hold_win": sum(win_hold) / len(win_hold) if win_hold else 0,
        "avg_hold_loss": sum(loss_hold) / len(loss_hold) if loss_hold else 0,
        "max_win": max(win_pnls) if win_pnls else 0,
        "max_loss": min(loss_pnls) if loss_pnls else 0,
        "consecutive_wins": _max_consecutive(completed, True),
        "consecutive_losses": _max_consecutive(completed, False),
        "signal_stats": signal_stats,
        "exit_reasons": exit_reasons,
        "hourly_pnl": hourly_pnl,
        "completed": completed,
    }


def _max_consecutive(completed, is_win):
    max_c = 0
    current = 0
    for _, t in completed:
        pnl = t["exit"].get("pnl", 0)
        if (is_win and pnl > 0) or (not is_win and pnl <= 0):
            current += 1
            max_c = max(max_c, current)
        else:
            current = 0
    return max_c


# ==================== 生成复盘报告 ====================

def format_report(today_stats, week_stats, all_trades):
    now = datetime.now(BJT)
    lines = []
    lines.append(f"📊 每日交易复盘报告")
    lines.append(f"日期: {now.strftime('%Y-%m-%d %H:%M')} 北京时间")
    lines.append(f"{'='*50}")

    # 今日统计
    if today_stats:
        s = today_stats
        lines.append(f"\n📌 今日交易 ({s['total']} 笔)")
        lines.append(f"  胜率: {s['win_rate']*100:.1f}% ({s['wins']}胜/{s['losses']}负)")
        lines.append(f"  总PnL: {s['total_pnl']:+.4f} USDT ({s['total_pnl_pct']:+.2f}%)")
        if s['avg_loss'] != 0:
            lines.append(f"  盈亏比: {abs(s['avg_win']/s['avg_loss']):.2f}:1")
        lines.append(f"  最大盈利: {s['max_win']:+.4f}  最大亏损: {s['max_loss']:+.4f}")
        lines.append(f"  连胜: {s['consecutive_wins']}  连亏: {s['consecutive_losses']}")
        lines.append(f"  平均持仓: 胜{s['avg_hold_win']:.0f}min 负{s['avg_hold_loss']:.0f}min")

        # 今日交易明细
        lines.append(f"\n  交易明细:")
        for tid, t in s["completed"]:
            entry = t["entry"]
            exit_ = t["exit"]
            pnl = exit_.get("pnl", 0)
            icon = "🟢" if pnl > 0 else "🔴"
            direction = entry.get("direction", "?")
            lines.append(f"    {icon} {tid} {direction.upper()} "
                        f"入{entry.get('entry_price',0):.1f}→出{exit_.get('exit_price',0):.1f} "
                        f"PnL={pnl:+.4f} ({exit_.get('pnl_pct',0):+.2f}%) "
                        f"持仓{exit_.get('hold_time_minutes',0):.0f}min "
                        f"出场={exit_.get('exit_reason','?')}")
    else:
        lines.append(f"\n📌 今日无交易")

    # 本周统计
    if week_stats and week_stats["total"] > 0:
        s = week_stats
        lines.append(f"\n📈 近 7 天统计 ({s['total']} 笔)")
        lines.append(f"  胜率: {s['win_rate']*100:.1f}% ({s['wins']}胜/{s['losses']}负)")
        lines.append(f"  总PnL: {s['total_pnl']:+.4f} USDT ({s['total_pnl_pct']:+.2f}%)")
        if s['avg_loss'] != 0:
            lines.append(f"  盈亏比: {abs(s['avg_win']/s['avg_loss']):.2f}:1")
        lines.append(f"  盈利因子: {s['profit_factor']:.2f}")

        # 信号源胜率
        if s["signal_stats"]:
            lines.append(f"\n  信号源表现:")
            for src, st in sorted(s["signal_stats"].items(), key=lambda x: -(x[1]["wins"]+x[1]["losses"])):
                t = st["wins"] + st["losses"]
                wr = st["wins"] / t * 100 if t else 0
                lines.append(f"    {src:20s}  {t}笔 胜率{wr:.0f}% PnL={st['pnl']:+.4f}")

        # 出场原因分布
        if s["exit_reasons"]:
            lines.append(f"\n  出场原因:")
            for reason, count in sorted(s["exit_reasons"].items(), key=lambda x: -x[1]):
                lines.append(f"    {reason:20s}  {count}笔 ({count/s['total']*100:.0f}%)")

        # 最佳/最差时段
        if s["hourly_pnl"]:
            lines.append(f"\n  时段表现:")
            sorted_hours = sorted(s["hourly_pnl"].items(), key=lambda x: x[1]["pnl"], reverse=True)
            for h, d in sorted_hours[:3]:
                icon = "🟢" if d["pnl"] > 0 else "🔴"
                lines.append(f"    {icon} {h}:00  {d['count']}笔 PnL={d['pnl']:+.4f}")

    return "\n".join(lines)


# ==================== AI 改进建议 ====================

def generate_ai_suggestions(report_text, week_stats):
    """用 AI 分析复盘数据，给出改进建议"""
    try:
        from config import AI_BASE_URL, AI_API_KEY, AI_MODEL, AI_ENABLED, AI_MAX_TOKENS
        if not AI_ENABLED or not AI_BASE_URL or not AI_API_KEY:
            return None

        import requests

        # 构建分析数据摘要
        data_summary = report_text
        if week_stats and week_stats.get("signal_stats"):
            data_summary += "\n\n信号源详细数据:\n"
            for src, st in week_stats["signal_stats"].items():
                t = st["wins"] + st["losses"]
                wr = st["wins"] / t * 100 if t else 0
                data_summary += f"- {src}: {t}笔 胜率{wr:.0f}% 总PnL={st['pnl']:+.4f}\n"

        if week_stats and week_stats.get("hourly_pnl"):
            data_summary += "\n时段表现:\n"
            for h, d in sorted(week_stats["hourly_pnl"].items()):
                data_summary += f"- {h}:00: {d['count']}笔 PnL={d['pnl']:+.4f}\n"

        prompt = f"""你是一位资深量化交易教练。请根据以下交易复盘数据，给出具体可执行的改进建议。

要求：
1. 找出最突出的问题（不多于 3 个）
2. 每个问题给出具体改进措施
3. 指出做得好的地方，鼓励正向行为
4. 建议要具体可执行，不要泛泛而谈
5. 用中文回答，简洁有力

复盘数据:
{data_summary}"""

        proxies = None
        try:
            from config import PROXY_ENABLED, SOCKS5_PROXY
            if PROXY_ENABLED and SOCKS5_PROXY:
                proxies = {"http": SOCKS5_PROXY, "https": SOCKS5_PROXY}
        except ImportError:
            pass

        headers = {
            "Authorization": f"Bearer {AI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": AI_MODEL,
            "messages": [
                {"role": "system", "content": "你是资深量化交易教练，擅长分析交易数据并给出改进建议。"},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": AI_MAX_TOKENS,
            "temperature": 0.7,
        }

        url = f"{AI_BASE_URL.rstrip('/')}/chat/completions"
        r = requests.post(url, json=payload, headers=headers, proxies=proxies, timeout=60)
        if r.status_code == 200:
            data = r.json()
            return data["choices"][0]["message"]["content"]
        else:
            print(f"  ⚠️ AI 请求失败: {r.status_code}")
            return None
    except Exception as e:
        print(f"  ⚠️ AI 建议生成失败: {e}")
        return None


# ==================== 邮件发送 ====================

def send_email(subject, body):
    """发送邮件"""
    try:
        from config import (
            EMAIL_ENABLED, EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT,
            EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER
        )

        if not EMAIL_ENABLED:
            print("  ⚠️ 邮件未启用，请在 config.py 中设置 EMAIL_ENABLED = True")
            return False

        if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER]):
            print("  ⚠️ 邮件配置不完整，请检查 config.py")
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER

        # 纯文本
        msg.attach(MIMEText(body, "plain", "utf-8"))

        # HTML 格式
        html_body = body.replace("\n", "<br>").replace(" ", "&nbsp;")
        html = f"""<html><body style="font-family:monospace;font-size:13px;background:#111;color:#eee;padding:20px;">
<pre style="white-space:pre-wrap;">{html_body}</pre></body></html>"""
        msg.attach(MIMEText(html, "html", "utf-8"))

        # 发送
        server = smtplib.SMTP_SSL(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT)
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        server.quit()

        print(f"  ✅ 邮件已发送至 {EMAIL_RECEIVER}")
        return True

    except Exception as e:
        print(f"  ❌ 邮件发送失败: {e}")
        return False


# ==================== 主流程 ====================

def run_daily_review(send_mail=True):
    now = datetime.now(BJT)
    print(f"\n{'='*50}")
    print(f"  📊 每日交易复盘 | {now.strftime('%Y-%m-%d %H:%M')} 北京时间")
    print(f"{'='*50}\n")

    # 加载数据
    records = load_journal()
    all_trades = group_trades(records)

    if not all_trades:
        print("  📭 没有交易记录")
        return

    today_trades = filter_today(all_trades)
    week_trades = filter_recent(all_trades, days=7)

    # 计算统计
    today_stats = compute_stats(today_trades) if today_trades else None
    week_stats = compute_stats(week_trades) if week_trades else None

    # 生成报告
    report = format_report(today_stats, week_stats, all_trades)
    print(report)

    # AI 改进建议
    print(f"\n🧠 AI 改进建议:")
    ai_suggestions = generate_ai_suggestions(report, week_stats)
    if ai_suggestions:
        print(ai_suggestions)
    else:
        print("  （AI 未启用或未返回结果，请在 config.py 中配置 AI_BASE_URL 和 AI_API_KEY）")

    # 保存报告到文件
    report_file = os.path.join(BASE_DIR, "daily_reviews", f"{now.strftime('%Y-%m-%d')}.txt")
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report)
        if ai_suggestions:
            f.write(f"\n\n{'='*50}\n🧠 AI 改进建议:\n")
            f.write(ai_suggestions)
    print(f"\n  💾 报告已保存: {report_file}")

    # 发送邮件
    if send_mail:
        full_report = report
        if ai_suggestions:
            full_report += f"\n\n{'='*50}\n🧠 AI 改进建议:\n" + ai_suggestions
        subject = f"📊 交易复盘 {now.strftime('%Y-%m-%d')}"
        send_email(subject, full_report)

    print(f"\n{'='*50}\n")


if __name__ == "__main__":
    if "--test-email" in sys.argv:
        print("📧 测试邮件发送...")
        send_email("测试邮件", "这是一封测试邮件，来自订单流交易系统。")
    elif "--no-email" in sys.argv:
        run_daily_review(send_mail=False)
    else:
        run_daily_review(send_mail=True)
