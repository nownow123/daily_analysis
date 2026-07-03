#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cloud runner for A-share tail-session candidate screening.

It is dependency-free and designed for GitHub Actions:
- run learning from prior candidate files;
- run today's tail-session picker;
- write Markdown/CSV outputs;
- keep a conservative adaptive rule file after enough samples.

Research boundary: this is a screening and review tool only. It never places
orders and does not guarantee returns.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
LEARNING_DIR = ROOT / "learning"
ADAPTIVE_RULES = LEARNING_DIR / "adaptive_rules.json"
SAMPLES_CSV = LEARNING_DIR / "samples.csv"
TZ = ZoneInfo("Asia/Shanghai")
UT = "bd1d9ddb04089700cf9c27f6f7426281"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/",
}

SPOT_FIELDS = ",".join(
    [
        "f12",
        "f14",
        "f2",
        "f3",
        "f5",
        "f6",
        "f7",
        "f8",
        "f10",
        "f15",
        "f16",
        "f17",
        "f18",
        "f20",
        "f21",
        "f100",
    ]
)

SAMPLE_FIELDS = [
    "sample_id",
    "status",
    "pick_date",
    "pick_time",
    "next_date",
    "code",
    "name",
    "industry",
    "conclusion",
    "score",
    "market_score",
    "sector_score",
    "trend_score",
    "quant_score",
    "intraday_score",
    "risk_score",
    "pct",
    "price",
    "volume_ratio",
    "turnover",
    "amount",
    "float_mv",
    "sector_rank",
    "late_return",
    "last5_return",
    "day_range_pos",
    "return_open",
    "return_0945",
    "return_1000",
    "best_return_before_1000",
    "worst_return_before_1000",
    "net_return_1000",
    "flags",
    "updated_at",
    "note",
]


def now_cn() -> dt.datetime:
    return dt.datetime.now(TZ)


def today() -> str:
    return now_cn().strftime("%Y-%m-%d")


def stamp() -> str:
    return now_cn().strftime("%Y%m%d_%H%M%S")


def fetch_json(url: str, retries: int = 3, timeout: int = 20) -> dict:
    urls = [url]
    if url.startswith("http://"):
        urls.append("https://" + url[len("http://") :])
    elif url.startswith("https://"):
        urls.append("http://" + url[len("https://") :])
    last_error: Exception | None = None
    for candidate in urls:
        for attempt in range(retries):
            try:
                req = urllib.request.Request(candidate, headers=HEADERS)
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    return json.loads(response.read().decode("utf-8", errors="replace"))
            except Exception as exc:
                last_error = exc
                time.sleep(0.8 * (attempt + 1))
    raise RuntimeError(f"market data request failed: {last_error}")


def f(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return None if isinstance(value, float) and math.isnan(value) else float(value)
    text = str(value).strip()
    if not text or text in {"-", "--", "nan", "None"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def mean(values):
    values = [f(v) for v in values]
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None


def median(values):
    values = sorted(v for v in (f(x) for x in values) if v is not None)
    if not values:
        return None
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2


def pct(price, base):
    price = f(price)
    base = f(base)
    if price is None or base in (None, 0):
        return None
    return (price / base - 1) * 100


def fmt_pct(value):
    value = f(value)
    return "-" if value is None else f"{value:.2f}%"


def fmt_num(value, digits=2):
    value = f(value)
    return "-" if value is None else f"{value:.{digits}f}"


def fmt_yi(value):
    value = f(value)
    return "-" if value is None else f"{value / 100000000:.1f}亿"


def secid(code: str) -> str:
    return f"1.{code}" if code.startswith(("5", "6", "9")) else f"0.{code}"


def board(code: str) -> str:
    if code.startswith(("300", "301")):
        return "创业板"
    if code.startswith(("688", "689")):
        return "科创板"
    if code.startswith(("600", "601", "603", "605")):
        return "沪主板"
    if code.startswith(("000", "001", "002", "003")):
        return "深主板"
    if code.startswith(("8", "4", "920")):
        return "北交所"
    return "其他"


def is_dual(code: str) -> bool:
    return board(code) in {"创业板", "科创板"}


def normalize(row: dict) -> dict:
    code = str(row.get("f12") or "").strip()
    return {
        "code": code,
        "name": str(row.get("f14") or "").strip(),
        "board": board(code),
        "price": f(row.get("f2")),
        "pct": f(row.get("f3")),
        "amount": f(row.get("f6")),
        "turnover": f(row.get("f8")),
        "volume_ratio": f(row.get("f10")),
        "high": f(row.get("f15")),
        "low": f(row.get("f16")),
        "open": f(row.get("f17")),
        "preclose": f(row.get("f18")),
        "float_mv": f(row.get("f21")),
        "industry": str(row.get("f100") or "未分类").strip() or "未分类",
    }


def fetch_spot() -> list[dict]:
    params = {
        "pn": 1,
        "pz": 6000,
        "po": 1,
        "np": 1,
        "ut": UT,
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": SPOT_FIELDS,
    }
    url = "http://push2.eastmoney.com/api/qt/clist/get?" + urllib.parse.urlencode(params)
    rows = (fetch_json(url).get("data") or {}).get("diff") or []
    stocks = [normalize(row) for row in rows]
    clean = []
    for item in stocks:
        name = item["name"].upper()
        if item["board"] == "其他" or item["board"] == "北交所":
            continue
        if "ST" in name or "退" in name or item["name"].startswith(("N", "C")):
            continue
        if item["price"] in (None, 0) or item["pct"] is None:
            continue
        clean.append(item)
    return clean


def sector_stats(stocks: list[dict]) -> tuple[dict, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for stock in stocks:
        groups.setdefault(stock["industry"], []).append(stock)
    ranked = []
    for industry, items in groups.items():
        pcts = [x["pct"] for x in items if x["pct"] is not None]
        if len(pcts) < 3:
            continue
        stat = {
            "industry": industry,
            "count": len(items),
            "avg_pct": mean(pcts),
            "adv_ratio": sum(1 for x in pcts if x > 0) / len(pcts),
            "strong_count": sum(1 for x in pcts if x >= 3),
        }
        ranked.append(stat)
    ranked.sort(key=lambda x: (x["avg_pct"] or -99, x["strong_count"]), reverse=True)
    by_name = {}
    for index, item in enumerate(ranked, 1):
        item["rank"] = index
        by_name[item["industry"]] = item
    return by_name, ranked


def score_market(stocks: list[dict], sectors: list[dict]) -> dict:
    pcts = [x["pct"] for x in stocks if x["pct"] is not None]
    adv_ratio = sum(1 for x in pcts if x > 0) / len(pcts) if pcts else 0
    avg_pct = mean(pcts) or 0
    top_avg = sectors[0]["avg_pct"] if sectors else 0
    strong_sector_count = sum(1 for x in sectors[:20] if (x["avg_pct"] or 0) >= 2)
    score = 0
    score += 8 if adv_ratio >= 0.58 else 6 if adv_ratio >= 0.52 else 4 if adv_ratio >= 0.47 else 2 if adv_ratio >= 0.42 else 0
    score += 5 if avg_pct >= 0.8 else 4 if avg_pct >= 0.4 else 2 if avg_pct >= 0 else 0
    score += 4 if top_avg >= 3 else 3 if top_avg >= 2 else 2 if top_avg >= 1 else 0
    score += 3 if strong_sector_count >= 3 else 2 if strong_sector_count >= 2 else 1 if strong_sector_count >= 1 else 0
    score = min(score, 20)
    label = "顺风" if score >= 16 else "中性偏强" if score >= 12 else "中性偏弱" if score >= 8 else "逆风"
    return {"score": score, "label": label, "adv_ratio": adv_ratio, "avg_pct": avg_pct}


def score_sector(stat: dict | None) -> int:
    if not stat:
        return 0
    score = 0
    rank = stat["rank"]
    avg_pct = stat["avg_pct"] or 0
    adv_ratio = stat["adv_ratio"] or 0
    strong = stat["strong_count"] or 0
    score += 8 if rank <= 3 else 6 if rank <= 5 else 4 if rank <= 10 else 2 if rank <= 20 else 0
    score += 5 if avg_pct >= 4 else 4 if avg_pct >= 3 else 3 if avg_pct >= 2 else 2 if avg_pct >= 1 else 1 if avg_pct > 0 else 0
    score += 4 if adv_ratio >= 0.8 else 3 if adv_ratio >= 0.65 else 2 if adv_ratio >= 0.5 else 0
    score += 3 if strong >= 5 else 2 if strong >= 3 else 1 if strong >= 1 else 0
    return min(score, 20)


def fetch_kline(code: str) -> list[dict]:
    begin = (now_cn() - dt.timedelta(days=200)).strftime("%Y%m%d")
    params = {
        "secid": secid(code),
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101,
        "fqt": 1,
        "beg": begin,
        "end": "20500101",
    }
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get?" + urllib.parse.urlencode(params)
    lines = (fetch_json(url).get("data") or {}).get("klines") or []
    rows = []
    for line in lines:
        part = line.split(",")
        if len(part) >= 11:
            rows.append(
                {
                    "date": part[0],
                    "open": f(part[1]),
                    "close": f(part[2]),
                    "high": f(part[3]),
                    "low": f(part[4]),
                    "volume": f(part[5]),
                    "amount": f(part[6]),
                    "pct": f(part[8]),
                    "turnover": f(part[10]),
                }
            )
    return rows


def fetch_trends(code: str, ndays: int = 1) -> list[dict]:
    params = {
        "secid": secid(code),
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ndays": ndays,
        "iscr": 0,
        "iscca": 0,
    }
    url = "http://push2his.eastmoney.com/api/qt/stock/trends2/get?" + urllib.parse.urlencode(params)
    lines = (fetch_json(url).get("data") or {}).get("trends") or []
    rows = []
    for line in lines:
        part = line.split(",")
        if len(part) < 8:
            continue
        try:
            ts = dt.datetime.strptime(part[0], "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
        except ValueError:
            continue
        rows.append(
            {
                "time": ts,
                "open": f(part[1]),
                "close": f(part[2]),
                "high": f(part[3]),
                "low": f(part[4]),
                "volume": f(part[5]),
                "amount": f(part[6]),
                "avg_price": f(part[7]),
            }
        )
    return rows


def score_quant(stock: dict) -> tuple[int, list[str]]:
    flags = []
    score = 0
    p = stock["pct"]
    vr = stock["volume_ratio"]
    turn = stock["turnover"]
    mv = stock["float_mv"]
    amount = stock["amount"]
    if is_dual(stock["code"]):
        score += 4 if p is not None and 3 <= p <= 8 else 2 if p is not None and 2 <= p <= 10 else 0
        if p is None or not (3 <= p <= 8):
            flags.append("涨幅不在双创优先区间")
        score += 3 if turn is not None and 5 <= turn <= 15 else 1 if turn is not None and 3 <= turn <= 20 else 0
    else:
        score += 4 if p is not None and 2 <= p <= 6 else 2 if p is not None and 1.5 <= p <= 8 else 0
        if p is None or not (2 <= p <= 6):
            flags.append("涨幅不在主板优先区间")
        score += 3 if turn is not None and 4 <= turn <= 12 else 1 if turn is not None and 2.5 <= turn <= 16 else 0
    if turn is None or turn < 2.5 or turn > 20:
        flags.append("换手不适合")
    score += 3 if vr is not None and 1.2 <= vr <= 3.5 else 2 if vr is not None and 1 <= vr <= 6 else 0
    if vr is None or vr < 1:
        flags.append("量比小于1")
    elif vr > 3.5:
        flags.append("量比偏高")
    score += 3 if mv is not None and 8e9 <= mv <= 18e9 else 2 if mv is not None and 5e9 <= mv <= 25e9 else 1 if mv is not None and 3e9 <= mv <= 30e9 else 0
    if mv is None or not (3e9 <= mv <= 30e9):
        flags.append("流通市值不适合")
    score += 2 if amount is not None and amount >= 5e8 else 1 if amount is not None and amount >= 3e8 else 0
    if amount is None or amount < 3e8:
        flags.append("成交额不足3亿")
    return min(score, 15), flags


def score_trend(stock: dict, kline: list[dict]) -> tuple[int, list[str], dict]:
    flags = []
    detail = {}
    closes = [row["close"] for row in kline if row["close"] is not None]
    volumes = [row["volume"] for row in kline if row["volume"] is not None]
    price = stock["price"]
    if len(closes) < 20 or price is None:
        return 5, ["日K数据不足"], detail
    ma5, ma10, ma20 = mean(closes[-5:]), mean(closes[-10:]), mean(closes[-20:])
    score = 0
    if ma20 and price > ma20:
        score += 5
    else:
        flags.append("未站上20日线")
    if ma5 and ma10 and ma20 and ma5 >= ma10 >= ma20:
        score += 5
    elif ma5 and ma10 and ma5 >= ma10:
        score += 3
    else:
        flags.append("短均线结构不强")
    if len(volumes) >= 6 and mean(volumes[-6:-1]):
        vol_ratio = volumes[-1] / mean(volumes[-6:-1])
        detail["vol_vs_ma5"] = vol_ratio
        score += 2 if vol_ratio >= 1.15 else 1 if vol_ratio >= 0.85 else 0
        if vol_ratio < 0.85:
            flags.append("日K量能未放大")
    if stock["high"] and stock["low"] and stock["high"] > stock["low"]:
        pos = (price - stock["low"]) / (stock["high"] - stock["low"])
        detail["day_range_pos"] = pos
        score += 3 if pos >= 0.78 else 1 if pos >= 0.6 else 0
        if pos < 0.6:
            flags.append("日内位置偏低")
    return min(score, 15), flags, detail


def score_intraday(stock: dict, trends: list[dict]) -> tuple[int, list[str], dict]:
    flags = []
    detail = {}
    if not trends:
        return 8, ["分时数据缺失"], detail
    last = trends[-1]
    last_price = last.get("close") or stock["price"]
    detail["last_time"] = last["time"].strftime("%H:%M")
    cutoff = last["time"].replace(hour=14, minute=30, second=0, microsecond=0)
    if last["time"] < cutoff:
        return 8, [f"当前分时到{detail['last_time']}，尚未进入14:30尾盘窗口"], detail
    late = [row for row in trends if row["time"] >= cutoff]
    late_start = late[0].get("close") or late[0].get("open")
    score = 0
    if last_price and last.get("avg_price") and last_price >= last["avg_price"]:
        score += 6
    else:
        flags.append("分时未站上均价线")
    late_return = pct(last_price, late_start)
    detail["late_return"] = late_return
    score += 4 if late_return is not None and late_return >= 0.8 else 3 if late_return is not None and late_return >= 0 else 1 if late_return is not None and late_return >= -0.5 else 0
    if late_return is not None and late_return < -0.5:
        flags.append("14:30后走弱")
    highs = [row["high"] for row in trends if row["high"] is not None]
    lows = [row["low"] for row in trends if row["low"] is not None]
    if highs and lows and max(highs) > min(lows) and last_price:
        pos = (last_price - min(lows)) / (max(highs) - min(lows))
        detail["intraday_range_pos"] = pos
        score += 4 if pos >= 0.8 else 3 if pos >= 0.65 else 1 if pos >= 0.5 else 0
        if pos < 0.5:
            flags.append("尾盘不在日内高位")
    if len(trends) >= 6 and last_price and trends[-6].get("close"):
        last5 = pct(last_price, trends[-6]["close"])
        detail["last5_return"] = last5
        recent_amount = mean([row["amount"] for row in trends[-5:]])
        avg_amount = mean([row["amount"] for row in trends])
        if last5 is not None and last5 > 2 and avg_amount and recent_amount and recent_amount > avg_amount * 2.5:
            flags.append("最后5分钟急拉放量")
        else:
            score += 4
    return min(score, 20), flags, detail


def load_rules() -> dict:
    if not ADAPTIVE_RULES.exists():
        return {}
    try:
        return json.loads(ADAPTIVE_RULES.read_text(encoding="utf-8"))
    except Exception:
        return {}


def adaptive_penalty(flags: list[str], rules: dict) -> tuple[int, list[str]]:
    penalty = 0
    hits = []
    for keyword, points in (rules.get("flag_penalties") or {}).items():
        if any(keyword in flag for flag in flags):
            point = int(points)
            penalty += point
            hits.append(f"{keyword}-{point}分")
    return min(penalty, 12), hits


def risk_score(flags: list[str]) -> int:
    high = ["ST", "退市", "流通市值不适合", "成交额不足", "最后5分钟急拉"]
    score = 10
    for flag in flags:
        score -= 3 if any(x in flag for x in high) else 1
    return max(score, 0)


def analyze_one(stock: dict, sector_by_name: dict, market: dict, rules: dict) -> dict:
    flags = []
    quant, q_flags = score_quant(stock)
    flags.extend(q_flags)
    try:
        trend, t_flags, detail = score_trend(stock, fetch_kline(stock["code"]))
    except Exception as exc:
        trend, t_flags, detail = 5, [f"日K获取失败:{exc}"], {}
    flags.extend(t_flags)
    try:
        intraday, i_flags, i_detail = score_intraday(stock, fetch_trends(stock["code"], 1))
    except Exception as exc:
        intraday, i_flags, i_detail = 8, [f"分时获取失败:{exc}"], {}
    flags.extend(i_flags)
    sector_stat = sector_by_name.get(stock["industry"])
    sector = score_sector(sector_stat)
    risk = risk_score(flags)
    penalty, hits = adaptive_penalty(flags, rules)
    score = max(0, min(100, market["score"] + sector + trend + quant + intraday + risk - penalty))
    high_risk = any(key in "；".join(flags) for key in ["分时未站上均价线", "未站上20日线", "最后5分钟急拉", "流通市值不适合", "成交额不足"])
    focus_min = int(rules.get("min_score_for_focus") or 80)
    conclusion = "市场逆风，默认空仓/只观察" if market["score"] < 8 else "重点观察" if score >= focus_min and not high_risk else "只观察" if score >= 70 else "暂不做"
    row = dict(stock)
    row.update(detail)
    row.update(i_detail)
    row.update(
        {
            "pick_date": today(),
            "pick_time": now_cn().strftime("%H:%M:%S"),
            "generated_at": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
            "market_score": market["score"],
            "sector_score": sector,
            "trend_score": trend,
            "quant_score": quant,
            "intraday_score": intraday,
            "risk_score": risk,
            "adaptive_penalty": penalty,
            "score": score,
            "conclusion": conclusion,
            "flags": "；".join(dict.fromkeys(flags)) if flags else "无明显扣分项",
            "adaptive_hits": "；".join(hits),
            "sector_rank": sector_stat["rank"] if sector_stat else "",
            "sector_avg_pct": sector_stat["avg_pct"] if sector_stat else "",
        }
    )
    return row


def initial_pool(stock: dict) -> bool:
    max_pct = 18 if is_dual(stock["code"]) else 10.5
    return (
        stock["pct"] is not None
        and 1 <= stock["pct"] <= max_pct
        and stock["amount"] is not None
        and stock["amount"] >= 1e8
        and stock["turnover"] is not None
        and stock["turnover"] >= 1
        and stock["float_mv"] is not None
        and 3e9 <= stock["float_mv"] <= 8e10
        and (stock["volume_ratio"] is None or stock["volume_ratio"] >= 0.7)
    )


def basic_rank(stock: dict, sector_by_name: dict, market: dict) -> int:
    quant, _ = score_quant(stock)
    return market["score"] + score_sector(sector_by_name.get(stock["industry"])) + quant


def write_candidates(rows: list[dict], market: dict, sectors: list[dict]) -> tuple[Path, Path]:
    out = OUTPUT_DIR / today()
    out.mkdir(parents=True, exist_ok=True)
    base = stamp()
    csv_path = out / f"candidates_{base}.csv"
    report_path = out / f"report_{base}.md"
    fields = [
        "pick_date",
        "pick_time",
        "generated_at",
        "score",
        "conclusion",
        "code",
        "name",
        "board",
        "industry",
        "pct",
        "price",
        "volume_ratio",
        "turnover",
        "amount",
        "float_mv",
        "sector_rank",
        "market_score",
        "sector_score",
        "trend_score",
        "quant_score",
        "intraday_score",
        "risk_score",
        "late_return",
        "last5_return",
        "day_range_pos",
        "intraday_range_pos",
        "flags",
        "adaptive_hits",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    lines = [
        "# A股尾盘选股报告",
        "",
        f"- 生成时间：{now_cn().strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"- 市场环境：{market['label']}，{market['score']}/20，上涨占比 {market['adv_ratio']:.1%}，平均涨幅 {market['avg_pct']:.2f}%",
        "- 边界：本报告只做公开行情筛选与研究排序，不构成买卖指令。",
        "",
        "## 强势板块 Top 5",
        "",
        "| 排名 | 板块 | 平均涨幅 | 上涨占比 | 强势个股数 |",
        "|---:|---|---:|---:|---:|",
    ]
    for item in sectors[:5]:
        lines.append(f"| {item['rank']} | {item['industry']} | {fmt_pct(item['avg_pct'])} | {item['adv_ratio']:.0%} | {item['strong_count']} |")
    lines += [
        "",
        "## 候选清单",
        "",
        "| 分数 | 结论 | 代码 | 名称 | 行业 | 涨幅 | 量比 | 换手 | 成交额 | 流通市值 | 风险 |",
        "|---:|---|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['score']} | {row['conclusion']} | {row['code']} | {row['name']} | {row['industry']} | "
            f"{fmt_pct(row['pct'])} | {fmt_num(row['volume_ratio'])} | {fmt_pct(row['turnover'])} | "
            f"{fmt_yi(row['amount'])} | {fmt_yi(row['float_mv'])} | {row['flags']} |"
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path, report_path


def run_picker(args) -> tuple[Path, Path]:
    stocks = fetch_spot()
    sector_by_name, sectors = sector_stats(stocks)
    market = score_market(stocks, sectors)
    rules = load_rules()
    pool = [stock for stock in stocks if initial_pool(stock)]
    pool.sort(key=lambda stock: basic_rank(stock, sector_by_name, market), reverse=True)
    pool = pool[: args.max_detail]
    rows = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(analyze_one, stock, sector_by_name, market, rules) for stock in pool]
        for future in as_completed(futures):
            try:
                rows.append(future.result())
            except Exception as exc:
                print(f"候选分析失败：{exc}", file=sys.stderr)
    rows.sort(key=lambda row: row["score"], reverse=True)
    return write_candidates(rows[: args.top], market, sectors)


def read_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def latest_candidate_files() -> list[Path]:
    files = sorted(OUTPUT_DIR.glob("*/candidates_*.csv"))
    by_date = {}
    for path in files:
        if path.parent.name >= today():
            continue
        by_date[path.parent.name] = path
    return [by_date[key] for key in sorted(by_date)]


def load_samples() -> list[dict]:
    if not SAMPLES_CSV.exists():
        return []
    return read_csv(SAMPLES_CSV)


def next_row(kline: list[dict], pick_date: str) -> dict | None:
    for index, row in enumerate(kline):
        if row["date"] == pick_date and index + 1 < len(kline):
            return kline[index + 1]
    return None


def price_before(rows: list[dict], target: dt.time) -> float | None:
    rows = [row for row in rows if row["time"].time() <= target and row.get("close") is not None]
    return rows[-1]["close"] if rows else None


def sample_for(path: Path, row: dict, cost_pct: float) -> dict:
    pick_date = row.get("pick_date") or path.parent.name
    pick_time = row.get("pick_time") or ""
    code = row["code"]
    sample_id = f"{pick_date}|{pick_time}|{path.name}|{code}"
    sample = {key: "" for key in SAMPLE_FIELDS}
    for key in SAMPLE_FIELDS:
        if key in row:
            sample[key] = row[key]
    sample.update({"sample_id": sample_id, "pick_date": pick_date, "pick_time": pick_time, "updated_at": now_cn().strftime("%Y-%m-%d %H:%M:%S")})
    buy = f(row.get("price"))
    if buy in (None, 0):
        sample.update({"status": "invalid", "note": "missing buy benchmark"})
        return sample
    try:
        nr = next_row(fetch_kline(code), pick_date)
    except Exception as exc:
        sample.update({"status": "error", "note": f"kline failed: {exc}"})
        return sample
    if not nr:
        sample.update({"status": "pending_next_day", "note": "no next trading day data yet"})
        return sample
    sample["next_date"] = nr["date"]
    sample["return_open"] = pct(nr.get("open"), buy)
    try:
        trends = [x for x in fetch_trends(code, 10) if x["time"].strftime("%Y-%m-%d") == nr["date"]]
    except Exception as exc:
        trends = []
        sample["note"] = f"trend failed: {exc}"
    if not trends:
        sample["status"] = "daily_only"
        return sample
    p0945 = price_before(trends, dt.time(9, 45))
    p1000 = price_before(trends, dt.time(10, 0))
    closes = [x["close"] for x in trends if dt.time(9, 30) <= x["time"].time() <= dt.time(10, 0) and x.get("close") is not None]
    sample["return_0945"] = pct(p0945, buy)
    sample["return_1000"] = pct(p1000, buy)
    sample["best_return_before_1000"] = pct(max(closes), buy) if closes else ""
    sample["worst_return_before_1000"] = pct(min(closes), buy) if closes else ""
    if sample["return_1000"] in ("", None):
        sample["status"] = "intraday_pending"
    else:
        sample["status"] = "done"
        sample["net_return_1000"] = f(sample["return_1000"]) - cost_pct
    return sample


def summarize(rows: list[dict], field: str = "net_return_1000") -> dict:
    vals = [f(row.get(field)) for row in rows]
    vals = [v for v in vals if v is not None]
    if not vals:
        return {"count": 0, "avg": None, "win_rate": None}
    return {"count": len(vals), "avg": mean(vals), "win_rate": sum(1 for v in vals if v > 0) / len(vals)}


def learn(cost_pct: float, min_samples: int) -> Path:
    existing = {row["sample_id"]: row for row in load_samples() if row.get("sample_id")}
    updates = []
    for candidate_file in latest_candidate_files():
        for row in read_csv(candidate_file):
            updates.append(sample_for(candidate_file, row, cost_pct))
    for row in updates:
        existing[row["sample_id"]] = row
    samples = sorted(existing.values(), key=lambda row: row.get("sample_id", ""))
    write_csv(SAMPLES_CSV, samples, SAMPLE_FIELDS)
    done = [row for row in samples if row.get("status") == "done" and f(row.get("net_return_1000")) is not None]
    overall = summarize(done)
    win_rate_text = "-" if overall["win_rate"] is None else f"{overall['win_rate']:.1%}"
    rules = None
    if len(done) >= min_samples:
        flag_groups = {}
        for row in done:
            for flag in str(row.get("flags", "")).split("；"):
                if flag and flag != "无明显扣分项":
                    flag_groups.setdefault(flag, []).append(row)
        penalties = {}
        for flag, rows in flag_groups.items():
            stat = summarize(rows)
            if stat["count"] >= max(5, int(len(done) * 0.08)) and stat["avg"] is not None and stat["avg"] < -0.25:
                penalties[flag] = 4 if stat["avg"] < -1 else 3 if stat["avg"] < -0.6 else 2
        focus = [row for row in done if (f(row.get("score")) or 0) >= 80]
        focus_stat = summarize(focus)
        focus_min = 84 if focus_stat["count"] >= 10 and ((focus_stat["avg"] or 0) < 0 or (focus_stat["win_rate"] or 0) < 0.45) else 82 if focus_stat["count"] >= 10 and ((focus_stat["avg"] or 0) < 0.15 or (focus_stat["win_rate"] or 0) < 0.5) else 80
        rules = {"generated_at": now_cn().strftime("%Y-%m-%d %H:%M:%S"), "sample_count": len(done), "min_score_for_focus": focus_min, "flag_penalties": penalties}
        ADAPTIVE_RULES.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")
    path = LEARNING_DIR / f"learning_report_{stamp()}.md"
    lines = [
        "# 策略学习报告",
        "",
        f"- 生成时间：{now_cn().strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"- 本次扫描候选样本：{len(updates)}",
        f"- 已完成10:00复盘样本：{len(done)}",
        f"- 平均净收益：{fmt_pct(overall['avg'])}",
        f"- 胜率：{win_rate_text}",
        "",
        "## 优化状态",
        "",
    ]
    if rules:
        lines.append(f"- 已生成自适应规则，重点观察门槛：{rules['min_score_for_focus']} 分。")
        if rules["flag_penalties"]:
            lines.append("- 自适应扣分：" + "，".join(f"{k}-{v}分" for k, v in rules["flag_penalties"].items()))
    else:
        lines.append(f"- 完成样本少于 {min_samples}，暂不自动调整参数。")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def run_daily(args) -> None:
    LEARNING_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    learning_report = learn(args.cost_pct, args.min_samples)
    csv_path, report_path = run_picker(args)
    print(f"LEARNING_REPORT={learning_report}")
    print(f"CANDIDATES_CSV={csv_path}")
    print(f"REPORT={report_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    daily = sub.add_parser("daily")
    daily.add_argument("--top", type=int, default=30)
    daily.add_argument("--max-detail", type=int, default=160)
    daily.add_argument("--workers", type=int, default=8)
    daily.add_argument("--cost-pct", type=float, default=0.15)
    daily.add_argument("--min-samples", type=int, default=30)
    pick = sub.add_parser("pick")
    pick.add_argument("--top", type=int, default=30)
    pick.add_argument("--max-detail", type=int, default=160)
    pick.add_argument("--workers", type=int, default=8)
    learn_cmd = sub.add_parser("learn")
    learn_cmd.add_argument("--cost-pct", type=float, default=0.15)
    learn_cmd.add_argument("--min-samples", type=int, default=30)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "daily":
            run_daily(args)
        elif args.command == "pick":
            csv_path, report_path = run_picker(args)
            print(f"CANDIDATES_CSV={csv_path}")
            print(f"REPORT={report_path}")
        elif args.command == "learn":
            print(f"LEARNING_REPORT={learn(args.cost_pct, args.min_samples)}")
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
