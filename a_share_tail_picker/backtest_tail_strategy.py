#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Historical replay for the A-share tail-session strategy.

The live picker is a point-in-time 14:45 scanner. Historical replay is harder
because public quote APIs usually do not expose a full-market 14:45 snapshot.
This script uses:

- current active A-share universe from the live quote API;
- Tencent daily kline for all-stock historical prefiltering;
- Yahoo 5-minute bars for candidates to approximate the 14:45 snapshot and
  the next trading day's 10:00 evaluation.

It is a research/backtest tool only. It does not place orders and does not
guarantee future returns.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import random
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from zoneinfo import ZoneInfo

import cloud_tail_picker as picker


ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / "data_cache"
DAILY_CACHE_DIR = CACHE_DIR / "tencent_daily"
YAHOO_DAILY_CACHE_DIR = CACHE_DIR / "yahoo_daily"
MINUTE_CACHE_DIR = CACHE_DIR / "yahoo_5m"
TENCENT_MINUTE_CACHE_DIR = CACHE_DIR / "tencent_5m"
BACKTEST_DIR = ROOT / "backtests"
TZ = ZoneInfo("Asia/Shanghai")
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
MINUTE_BARS_PER_DAY = 50


TRADE_FIELDS = [
    "pick_date",
    "rank",
    "code",
    "name",
    "board",
    "industry",
    "stock_url",
    "score",
    "conclusion",
    "data_quality",
    "buy_price",
    "next_date",
    "return_open",
    "return_0945",
    "return_1000",
    "best_return_before_1000",
    "worst_return_before_1000",
    "net_return_1000",
    "market_label",
    "market_score",
    "sector_score",
    "trend_score",
    "quant_score",
    "intraday_score",
    "risk_score",
    "pct",
    "volume_ratio",
    "turnover",
    "amount",
    "float_mv",
    "late_return",
    "last5_return",
    "day_range_pos",
    "intraday_range_pos",
    "short_reason",
    "short_blockers",
    "flags",
]

DAILY_FIELDS = [
    "pick_date",
    "market_label",
    "market_score",
    "universe_count",
    "prefilter_count",
    "strict_count",
    "selected_count",
    "avg_return_1000",
    "avg_net_return_1000",
    "win_rate",
    "best_stock_return",
    "worst_stock_return",
    "selected",
]


def parse_date(text: str) -> dt.date:
    return dt.datetime.strptime(text, "%Y-%m-%d").date()


def stamp() -> str:
    return dt.datetime.now(TZ).strftime("%Y%m%d_%H%M%S")


def cn_now_text() -> str:
    return dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def f(value):
    return picker.f(value)


def mean(values):
    return picker.mean(values)


def fmt_pct(value):
    return picker.fmt_pct(value)


def fmt_num(value, digits=2):
    return picker.fmt_num(value, digits)


def fmt_yi(value):
    return picker.fmt_yi(value)


def safe_float(value) -> float | None:
    value = f(value)
    if value is None or math.isnan(value):
        return None
    return value


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def yahoo_symbol(code: str) -> str:
    return f"{code}.SS" if code.startswith(("5", "6", "9")) else f"{code}.SZ"


def trading_time(ts: dt.datetime) -> bool:
    t = ts.time()
    return dt.time(9, 30) <= t <= dt.time(11, 30) or dt.time(13, 0) <= t <= dt.time(15, 0)


def request_json(url: str, retries: int = 3, timeout: int = 18) -> dict:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=YAHOO_HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
        except Exception as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(min(4, 0.8 * (attempt + 1)) + random.random() * 0.2)
    raise RuntimeError(str(last_error))


def fetch_daily_tencent_cached(code: str, days: int, refresh: bool = False) -> list[dict]:
    DAILY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = DAILY_CACHE_DIR / f"{code}_{days}.json"
    if path.exists() and not refresh:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    rows = picker.fetch_kline_tencent(code, days=days)
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return rows


def fetch_daily_yahoo_cached(code: str, days: int, refresh: bool = False) -> list[dict]:
    YAHOO_DAILY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = YAHOO_DAILY_CACHE_DIR / f"{code}_{days}.json"
    if path.exists() and not refresh:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    end = dt.datetime.now(TZ).date() + dt.timedelta(days=1)
    start = end - dt.timedelta(days=max(120, days * 3))
    start_dt = dt.datetime.combine(start, dt.time(0, 0), tzinfo=TZ)
    end_dt = dt.datetime.combine(end, dt.time(0, 0), tzinfo=TZ)
    params = {
        "period1": int(start_dt.timestamp()),
        "period2": int(end_dt.timestamp()),
        "interval": "1d",
        "includePrePost": "false",
    }
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(yahoo_symbol(code))}?" + urllib.parse.urlencode(params)
    data = request_json(url, retries=3, timeout=20)
    chart = data.get("chart") or {}
    if chart.get("error"):
        raise RuntimeError(chart["error"])
    result = (chart.get("result") or [{}])[0]
    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    rows: list[dict] = []
    for index, raw_ts in enumerate(timestamps):
        ts = dt.datetime.fromtimestamp(raw_ts, TZ)
        close = safe_float((quote.get("close") or [None])[index])
        if close is None:
            continue
        volume_shares = safe_float((quote.get("volume") or [None])[index]) or 0.0
        rows.append(
            {
                "date": ts.strftime("%Y-%m-%d"),
                "open": safe_float((quote.get("open") or [None])[index]) or close,
                "close": close,
                "high": safe_float((quote.get("high") or [None])[index]) or close,
                "low": safe_float((quote.get("low") or [None])[index]) or close,
                "volume": volume_shares / 100,
                "amount": None,
                "pct": None,
                "turnover": None,
                "source": "yahoo_daily",
            }
        )
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return rows


def fetch_daily_cached(code: str, days: int, refresh: bool = False) -> list[dict]:
    try:
        return fetch_daily_tencent_cached(code, days, refresh)
    except Exception as exc:
        print(f"Tencent daily failed {code}: {exc}; fallback to Yahoo daily", file=sys.stderr)
        return fetch_daily_yahoo_cached(code, days, refresh)


def fetch_minute_yahoo_cached(code: str, start: dt.date, end: dt.date, refresh: bool = False) -> list[dict]:
    MINUTE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = f"{code}_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.json"
    path = MINUTE_CACHE_DIR / key
    if path.exists() and not refresh:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass

    start_dt = dt.datetime.combine(start, dt.time(0, 0), tzinfo=TZ)
    end_dt = dt.datetime.combine(end + dt.timedelta(days=1), dt.time(0, 0), tzinfo=TZ)
    params = {
        "period1": int(start_dt.timestamp()),
        "period2": int(end_dt.timestamp()),
        "interval": "5m",
        "includePrePost": "false",
    }
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(yahoo_symbol(code))}?" + urllib.parse.urlencode(params)
    data = request_json(url, retries=3, timeout=20)
    chart = data.get("chart") or {}
    if chart.get("error"):
        raise RuntimeError(chart["error"])
    result = (chart.get("result") or [{}])[0]
    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    rows: list[dict] = []
    for index, raw_ts in enumerate(timestamps):
        ts = dt.datetime.fromtimestamp(raw_ts, TZ)
        if not trading_time(ts):
            continue
        close = safe_float((quote.get("close") or [None])[index])
        if close is None:
            continue
        volume = safe_float((quote.get("volume") or [None])[index]) or 0.0
        row = {
            "time": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "open": safe_float((quote.get("open") or [None])[index]) or close,
            "high": safe_float((quote.get("high") or [None])[index]) or close,
            "low": safe_float((quote.get("low") or [None])[index]) or close,
            "close": close,
            "volume": volume,
            "amount": volume * close if close is not None else None,
        }
        rows.append(row)
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return rows


def fetch_minute_tencent_chunk(code: str, end_marker: str) -> list[dict]:
    symbol = picker.tencent_symbol(code)
    url = f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={symbol},m5,{end_marker},320"
    req = urllib.request.Request(url, headers={"User-Agent": YAHOO_HEADERS["User-Agent"], "Referer": "https://gu.qq.com/"})
    with urllib.request.urlopen(req, timeout=18) as response:
        data = json.loads(response.read().decode("utf-8", errors="replace"))
    payload = ((data.get("data") or {}).get(symbol) or {})
    raw_rows = payload.get("m5") or []
    rows = []
    for part in raw_rows:
        if len(part) < 6:
            continue
        try:
            ts = dt.datetime.strptime(part[0], "%Y%m%d%H%M").replace(tzinfo=TZ)
        except ValueError:
            continue
        close = safe_float(part[2])
        if close is None:
            continue
        volume_shares = (safe_float(part[5]) or 0.0) * 100
        rows.append(
            {
                "time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "open": safe_float(part[1]) or close,
                "high": safe_float(part[3]) or close,
                "low": safe_float(part[4]) or close,
                "close": close,
                "volume": volume_shares,
                "amount": volume_shares * close,
                "source": "tencent_m5",
            }
        )
    return rows


def fetch_minute_tencent_cached(code: str, start: dt.date, end: dt.date, refresh: bool = False) -> list[dict]:
    if end < start:
        return []
    TENCENT_MINUTE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = f"{code}_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.json"
    path = TENCENT_MINUTE_CACHE_DIR / key
    if path.exists() and not refresh:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass

    collected: dict[str, dict] = {}
    marker_date = end + dt.timedelta(days=1)
    end_marker = marker_date.strftime("%Y%m%d0000")
    previous_first = ""
    for _ in range(40):
        chunk = fetch_minute_tencent_chunk(code, end_marker)
        if not chunk:
            break
        first_time = chunk[0]["time"]
        if first_time == previous_first:
            break
        previous_first = first_time
        for row in chunk:
            row_day = row["time"][:10]
            if start.strftime("%Y-%m-%d") <= row_day <= end.strftime("%Y-%m-%d"):
                collected[row["time"]] = row
        first_dt = dt.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
        if first_dt.date() <= start:
            break
        end_marker = first_dt.strftime("%Y%m%d%H%M")
        time.sleep(0.05)

    rows = [collected[key] for key in sorted(collected)]
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return rows


class MinuteStore:
    def __init__(self, start: dt.date, end: dt.date, refresh: bool = False) -> None:
        self.start = start
        self.end = end
        self.refresh = refresh
        self.recent_start = max(start, dt.datetime.now(TZ).date() - dt.timedelta(days=59))
        self.recent_cache: dict[str, list[dict]] = {}
        self.older_cache: dict[str, list[dict]] = {}
        self.failures: dict[str, str] = {}

    def recent_rows(self, code: str) -> list[dict]:
        if code in self.recent_cache:
            return self.recent_cache[code]
        key = f"recent:{code}"
        if key in self.failures:
            return []
        try:
            rows = fetch_minute_yahoo_cached(code, self.recent_start, self.end, self.refresh)
            self.recent_cache[code] = rows
            return rows
        except Exception as exc:
            try:
                rows = fetch_minute_tencent_cached(code, self.recent_start, self.end, self.refresh)
                self.recent_cache[code] = rows
                return rows
            except Exception as fallback_exc:
                self.failures[key] = f"yahoo: {exc}; tencent: {fallback_exc}"
                return []

    def older_rows(self, code: str) -> list[dict]:
        if code in self.older_cache:
            return self.older_cache[code]
        key = f"older:{code}"
        if key in self.failures:
            return []
        try:
            older_end = min(self.end, self.recent_start - dt.timedelta(days=1))
            rows = fetch_minute_tencent_cached(code, self.start, older_end, self.refresh)
            self.older_cache[code] = rows
            return rows
        except Exception as exc:
            self.failures[key] = str(exc)
            return []

    def rows_for_date(self, code: str, day: str, until: dt.time | None = None) -> list[dict]:
        day_date = parse_date(day)
        source_rows = self.recent_rows(code) if day_date >= self.recent_start else self.older_rows(code)
        rows = []
        for raw in source_rows:
            ts = dt.datetime.strptime(raw["time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
            if ts.strftime("%Y-%m-%d") != day:
                continue
            if until and ts.time() > until:
                continue
            row = dict(raw)
            row["time"] = ts
            rows.append(row)
        rows.sort(key=lambda row: row["time"])
        running_amount = 0.0
        running_volume = 0.0
        for row in rows:
            running_amount += f(row.get("amount")) or 0.0
            running_volume += f(row.get("volume")) or 0.0
            row["avg_price"] = running_amount / running_volume if running_volume else row.get("close")
        return rows


def enrich_daily_row(meta: dict, history: list[dict], index: int) -> dict:
    day = history[index]
    previous = history[index - 1] if index > 0 else {}
    close = f(day.get("close"))
    preclose = f(previous.get("close"))
    volume_hands = f(day.get("volume"))
    float_shares = f(meta.get("float_shares"))
    shares = volume_hands * 100 if volume_hands is not None else None
    amount = shares * close if shares is not None and close is not None else None
    turnover = shares / float_shares * 100 if shares is not None and float_shares else None
    prev_volumes = [f(row.get("volume")) for row in history[max(0, index - 5) : index]]
    volume_ratio = volume_hands / mean(prev_volumes) if volume_hands is not None and mean(prev_volumes) else None
    high = f(day.get("high"))
    low = f(day.get("low"))
    row = {
        "code": meta["code"],
        "name": meta["name"],
        "board": meta["board"],
        "stock_url": picker.stock_url(meta["code"]),
        "industry": meta.get("industry") or meta["board"],
        "price": close,
        "pct": picker.pct(close, preclose),
        "amount": amount,
        "turnover": turnover,
        "volume_ratio": volume_ratio,
        "high": high,
        "low": low,
        "open": f(day.get("open")),
        "preclose": preclose,
        "float_mv": float_shares * close if float_shares and close is not None else meta.get("float_mv"),
        "day_range_pos": picker.range_pos(close, high, low),
        "daily_volume_hands": volume_hands,
        "avg_prev5_volume_hands": mean(prev_volumes),
        "date": day["date"],
    }
    return row


def apply_minute_snapshot(stock: dict, rows: list[dict], pick_time: dt.time) -> dict:
    if not rows:
        return stock
    last = rows[-1]
    price = f(last.get("close"))
    high = max(f(row.get("high")) for row in rows if f(row.get("high")) is not None)
    low = min(f(row.get("low")) for row in rows if f(row.get("low")) is not None)
    first_open = f(rows[0].get("open"))
    volume_shares = sum(f(row.get("volume")) or 0.0 for row in rows)
    amount = sum(f(row.get("amount")) or 0.0 for row in rows)
    float_shares = None
    if f(stock.get("float_mv")) and f(stock.get("price")):
        float_shares = f(stock["float_mv"]) / f(stock["price"])
    elapsed = max(0.15, min(1.0, len(rows) / MINUTE_BARS_PER_DAY))
    prev5_shares = (f(stock.get("avg_prev5_volume_hands")) or 0.0) * 100
    volume_ratio = volume_shares / (prev5_shares * elapsed) if prev5_shares else stock.get("volume_ratio")
    updated = dict(stock)
    updated.update(
        {
            "price": price,
            "pct": picker.pct(price, stock.get("preclose")),
            "amount": amount,
            "turnover": volume_shares / float_shares * 100 if float_shares else stock.get("turnover"),
            "volume_ratio": volume_ratio,
            "high": high,
            "low": low,
            "open": first_open,
            "float_mv": float_shares * price if float_shares and price is not None else stock.get("float_mv"),
            "day_range_pos": picker.range_pos(price, high, low),
            "snapshot_time": pick_time.strftime("%H:%M"),
        }
    )
    return updated


def relaxed_prefilter(stock: dict) -> bool:
    dual = picker.is_dual(stock["code"])
    pct_ok = picker.between(stock.get("pct"), 1.0, 11.0) if dual else picker.between(stock.get("pct"), 0.5, 9.0)
    turn_ok = picker.between(stock.get("turnover"), 3.0, 22.0) if dual else picker.between(stock.get("turnover"), 2.0, 18.0)
    return (
        pct_ok
        and turn_ok
        and (f(stock.get("amount")) or 0) >= 1.5e8
        and picker.between(stock.get("float_mv"), 3e9, 4e10)
        and picker.between(stock.get("volume_ratio"), 0.8, 6.0)
        and picker.between(stock.get("day_range_pos"), 0.4, 1.05)
    )


def analyze_historical_one(
    stock: dict,
    history: list[dict],
    sector_by_name: dict,
    market: dict,
    rules: dict,
    minute_store: MinuteStore,
    pick_time: dt.time,
) -> dict:
    flags: list[str] = []
    minute_rows = minute_store.rows_for_date(stock["code"], stock["date"], until=pick_time)
    data_quality = "5分钟分时"
    if minute_rows:
        stock = apply_minute_snapshot(stock, minute_rows, pick_time)
    else:
        data_quality = "无分时"
        flags.append("历史5分钟分时缺失")

    quant, q_flags = picker.score_quant(stock)
    flags.extend(q_flags)
    trend, t_flags, detail = picker.score_trend(stock, history)
    flags.extend(t_flags)
    if minute_rows:
        intraday, i_flags, i_detail = picker.score_intraday(stock, minute_rows)
    else:
        intraday, i_flags, i_detail = 0, ["分时数据缺失"], {}
    flags.extend(i_flags)
    sector_stat = sector_by_name.get(stock["industry"])
    sector = picker.score_sector(sector_stat)
    risk = picker.risk_score(flags)
    penalty, hits = picker.adaptive_penalty(flags, rules)
    score = max(0, min(100, market["score"] + sector + trend + quant + intraday + risk - penalty))
    row = dict(stock)
    row.update(detail)
    row.update(i_detail)
    row.update(
        {
            "pick_date": stock["date"],
            "pick_time": pick_time.strftime("%H:%M:%S"),
            "market_score": market["score"],
            "market_label": market["label"],
            "sector_score": sector,
            "trend_score": trend,
            "quant_score": quant,
            "intraday_score": intraday,
            "risk_score": risk,
            "adaptive_penalty": penalty,
            "score": score,
            "flags": "；".join(dict.fromkeys(flags)) if flags else "无明显扣分项",
            "adaptive_hits": "；".join(hits),
            "sector_rank": sector_stat["rank"] if sector_stat else "",
            "sector_avg_pct": sector_stat["avg_pct"] if sector_stat else "",
            "data_quality": data_quality,
        }
    )
    focus_min = int(rules.get("min_score_for_focus") or 80)
    blockers = picker.strict_short_blockers(row, focus_min)
    row["short_blockers"] = "；".join(blockers)
    row["short_ready"] = "是" if not blockers else "否"
    row["conclusion"] = "短线候选" if row["short_ready"] == "是" else "不推荐"
    row["short_reason"] = picker.concise_reason(row)
    return row


def next_kline_row(history: list[dict], pick_date: str) -> dict | None:
    for index, row in enumerate(history):
        if row.get("date") == pick_date and index + 1 < len(history):
            return history[index + 1]
    return None


def price_before(rows: list[dict], target: dt.time) -> float | None:
    rows = [row for row in rows if row["time"].time() <= target and row.get("close") is not None]
    return rows[-1]["close"] if rows else None


def evaluate_trade(row: dict, history: list[dict], minute_store: MinuteStore, cost_pct: float) -> dict:
    trade = {key: "" for key in TRADE_FIELDS}
    for key in TRADE_FIELDS:
        if key in row:
            trade[key] = row[key]
    trade["buy_price"] = row.get("price")
    buy = f(row.get("price"))
    if buy in (None, 0):
        return trade
    next_row = next_kline_row(history, row["pick_date"])
    if not next_row:
        return trade
    trade["next_date"] = next_row["date"]
    trade["return_open"] = picker.pct(next_row.get("open"), buy)
    next_minutes = minute_store.rows_for_date(row["code"], next_row["date"], until=dt.time(10, 0))
    if next_minutes:
        p0945 = price_before(next_minutes, dt.time(9, 45))
        p1000 = price_before(next_minutes, dt.time(10, 0))
        highs = [f(x.get("high")) for x in next_minutes if f(x.get("high")) is not None]
        lows = [f(x.get("low")) for x in next_minutes if f(x.get("low")) is not None]
        trade["return_0945"] = picker.pct(p0945, buy)
        trade["return_1000"] = picker.pct(p1000, buy)
        trade["best_return_before_1000"] = picker.pct(max(highs), buy) if highs else ""
        trade["worst_return_before_1000"] = picker.pct(min(lows), buy) if lows else ""
        if f(trade["return_1000"]) is not None:
            trade["net_return_1000"] = f(trade["return_1000"]) - cost_pct
    return trade


def load_universe(max_codes: int = 0) -> list[dict]:
    stocks = picker.fetch_spot()
    universe = []
    for stock in stocks:
        price = f(stock.get("price"))
        float_mv = f(stock.get("float_mv"))
        if price in (None, 0) or not float_mv:
            continue
        item = dict(stock)
        item["float_shares"] = float_mv / price
        universe.append(item)
    universe.sort(key=lambda row: row["code"])
    if max_codes:
        universe = universe[:max_codes]
    return universe


def load_daily_histories(universe: list[dict], days: int, workers: int, refresh: bool) -> dict[str, list[dict]]:
    histories: dict[str, list[dict]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_daily_cached, row["code"], days, refresh): row["code"] for row in universe}
        total = len(futures)
        done = 0
        for future in as_completed(futures):
            code = futures[future]
            done += 1
            try:
                rows = future.result()
                if rows:
                    histories[code] = rows
            except Exception as exc:
                print(f"daily kline failed {code}: {exc}", file=sys.stderr)
            if done % 500 == 0 or done == total:
                print(f"daily kline loaded {done}/{total}, usable={len(histories)}", file=sys.stderr)
    return histories


def daily_stat(trades: list[dict]) -> dict:
    vals = [f(row.get("net_return_1000")) for row in trades]
    vals = [v for v in vals if v is not None]
    gross = [f(row.get("return_1000")) for row in trades]
    gross = [v for v in gross if v is not None]
    if not vals:
        return {"avg_net": None, "avg_gross": mean(gross), "win_rate": None, "best": None, "worst": None}
    return {
        "avg_net": mean(vals),
        "avg_gross": mean(gross),
        "win_rate": sum(1 for value in vals if value > 0) / len(vals),
        "best": max(vals),
        "worst": min(vals),
    }


def compound_daily(daily_rows: list[dict]) -> float | None:
    values = [f(row.get("avg_net_return_1000")) for row in daily_rows]
    values = [value for value in values if value is not None]
    if not values:
        return None
    acc = 1.0
    for value in values:
        acc *= 1 + value / 100
    return (acc - 1) * 100


def write_report(
    output_dir: Path,
    trade_rows: list[dict],
    daily_rows: list[dict],
    args: argparse.Namespace,
    universe_count: int,
    histories_count: int,
) -> Path:
    report_path = output_dir / f"backtest_summary_{stamp()}.md"
    done_trades = [row for row in trade_rows if f(row.get("net_return_1000")) is not None]
    stock_returns = [f(row.get("net_return_1000")) for row in done_trades]
    basket_returns = [f(row.get("avg_net_return_1000")) for row in daily_rows if f(row.get("avg_net_return_1000")) is not None]
    stock_avg = mean(stock_returns)
    basket_avg = mean(basket_returns)
    stock_win = sum(1 for value in stock_returns if value and value > 0) / len(stock_returns) if stock_returns else None
    day_win = sum(1 for value in basket_returns if value and value > 0) / len(basket_returns) if basket_returns else None
    compounded = compound_daily(daily_rows)
    lines = [
        "# 尾盘短线策略历史回放",
        "",
        f"- 生成时间：{cn_now_text()}",
        f"- 回放区间：{args.start} 至 {args.end}",
        f"- 买入时间假设：{args.pick_time}；卖出评估：次日 10:00 前，默认用 10:00 价格计算。",
        f"- 单笔成本扣减：{args.cost_pct:.2f}%",
        f"- 当前活跃沪深A股覆盖：{universe_count} 只；成功取得日K：{histories_count} 只。",
        f"- 每日最多选取：{args.top} 只严格候选。",
        "",
        "## 重要口径",
        "",
        "- 全市场初筛使用历史日K和当前流通股本近似，进入候选池后使用 Yahoo 5分钟线和腾讯历史5分钟线还原 14:45。",
        "- 因公开接口没有完整历史全市场 14:45 快照，换手、成交额和量比存在近似；报告已保留 `data_quality` 字段。",
        "- 当前活跃股票池会带来轻微幸存者偏差，退市或长期停牌股票不在样本内。",
        "- 本报告只做策略复盘和研究，不构成买卖指令。",
        "",
        "## 总体结果",
        "",
        f"- 有候选交易日：{len([row for row in daily_rows if int(row.get('selected_count') or 0) > 0])} 天",
        f"- 完成 10:00 评估股票数：{len(done_trades)} 只",
        f"- 单股平均净收益：{fmt_pct(stock_avg)}",
        f"- 单股胜率：{'-' if stock_win is None else f'{stock_win:.1%}'}",
        f"- 每日等权篮子平均净收益：{fmt_pct(basket_avg)}",
        f"- 每日篮子胜率：{'-' if day_win is None else f'{day_win:.1%}'}",
        f"- 区间日复利模拟：{fmt_pct(compounded)}",
        "",
        "## 每日结果",
        "",
        "| 日期 | 市场 | 候选数 | 平均10:00净收益 | 胜率 | 最好 | 最差 | 候选 |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in daily_rows:
        win_rate = f(row.get("win_rate"))
        win_rate_text = "-" if win_rate is None else f"{win_rate:.0%}"
        lines.append(
            f"| {row['pick_date']} | {row['market_label']} {row['market_score']}/20 | {row['selected_count']} | "
            f"{fmt_pct(row['avg_net_return_1000'])} | {win_rate_text} | "
            f"{fmt_pct(row['best_stock_return'])} | {fmt_pct(row['worst_stock_return'])} | {row['selected']} |"
        )
    lines.extend(
        [
            "",
            "## 前20笔明细",
            "",
            "| 日期 | 股票 | 分数 | 买入价 | 次日10:00 | 净收益 | 主要理由 |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in trade_rows[:20]:
        link = f"[{row['name']} {row['code']}]({row['stock_url']})"
        lines.append(
            f"| {row['pick_date']} | {link} | {fmt_num(row['score'], 0)} | {fmt_num(row['buy_price'])} | "
            f"{fmt_pct(row['return_1000'])} | {fmt_pct(row['net_return_1000'])} | {row.get('short_reason') or '-'} |"
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def run_backtest(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    start = parse_date(args.start)
    end = parse_date(args.end)
    minute_end = end + dt.timedelta(days=7)
    pick_time = dt.datetime.strptime(args.pick_time, "%H:%M").time()

    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = BACKTEST_DIR / f"{args.start}_to_{args.end}"
    output_dir.mkdir(parents=True, exist_ok=True)

    universe = load_universe(args.max_codes)
    print(f"universe loaded: {len(universe)}", file=sys.stderr)
    histories = load_daily_histories(universe, args.kline_days, args.workers, args.refresh_cache)
    meta_by_code = {row["code"]: row for row in universe}
    dates = sorted(
        {
            row["date"]
            for rows in histories.values()
            for row in rows
            if args.start <= row.get("date", "") <= args.end
        }
    )
    minute_store = MinuteStore(start, minute_end, refresh=args.refresh_cache)
    rules = picker.load_rules()
    trade_rows: list[dict] = []
    daily_rows: list[dict] = []
    min_universe_rows = min(1000, max(100, int(len(universe) * 0.5)))

    for pick_date in dates:
        stock_rows: list[dict] = []
        history_until: dict[str, list[dict]] = {}
        for code, history in histories.items():
            for index, row in enumerate(history):
                if row.get("date") != pick_date:
                    continue
                if index < 20:
                    continue
                meta = meta_by_code.get(code)
                if not meta:
                    continue
                stock_rows.append(enrich_daily_row(meta, history, index))
                history_until[code] = history[: index + 1]
                break
        if len(stock_rows) < min_universe_rows:
            continue
        sector_by_name, sectors = picker.sector_stats(stock_rows)
        market = picker.score_market(stock_rows, sectors)
        prefilter = [row for row in stock_rows if relaxed_prefilter(row)]
        prefilter.sort(key=lambda stock: picker.basic_rank(stock, sector_by_name, market), reverse=True)
        if args.max_detail and args.max_detail > 0:
            prefilter = prefilter[: args.max_detail]
        analyzed: list[dict] = []
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(
                    analyze_historical_one,
                    stock,
                    history_until[stock["code"]],
                    sector_by_name,
                    market,
                    rules,
                    minute_store,
                    pick_time,
                )
                for stock in prefilter
            ]
            for future in as_completed(futures):
                try:
                    analyzed.append(future.result())
                except Exception as exc:
                    print(f"analyze failed {pick_date}: {exc}", file=sys.stderr)
        strict = [row for row in analyzed if row.get("short_ready") == "是"]
        strict.sort(key=lambda row: row["score"], reverse=True)
        selected = strict[: args.top]
        day_trades: list[dict] = []
        for rank, row in enumerate(selected, 1):
            trade = evaluate_trade(row, histories[row["code"]], minute_store, args.cost_pct)
            trade["rank"] = rank
            day_trades.append(trade)
            trade_rows.append(trade)
        stat = daily_stat(day_trades)
        daily_rows.append(
            {
                "pick_date": pick_date,
                "market_label": market["label"],
                "market_score": market["score"],
                "universe_count": len(stock_rows),
                "prefilter_count": len(prefilter),
                "strict_count": len(strict),
                "selected_count": len(selected),
                "avg_return_1000": stat["avg_gross"],
                "avg_net_return_1000": stat["avg_net"],
                "win_rate": stat["win_rate"],
                "best_stock_return": stat["best"],
                "worst_stock_return": stat["worst"],
                "selected": "，".join(f"{row['name']} {row['code']}" for row in selected),
            }
        )
        print(
            f"{pick_date}: universe={len(stock_rows)} prefilter={len(prefilter)} strict={len(strict)} selected={len(selected)} avg_net={fmt_pct(stat['avg_net'])}",
            file=sys.stderr,
        )

    trade_path = output_dir / f"backtest_trades_{stamp()}.csv"
    daily_path = output_dir / f"backtest_daily_{stamp()}.csv"
    write_csv(trade_path, trade_rows, TRADE_FIELDS)
    write_csv(daily_path, daily_rows, DAILY_FIELDS)
    report_path = write_report(output_dir, trade_rows, daily_rows, args, len(universe), len(histories))
    return trade_path, daily_path, report_path


def default_dates() -> tuple[str, str]:
    today = dt.datetime.now(TZ).date()
    start = today.replace(day=1) - dt.timedelta(days=1)
    start = start.replace(day=1)
    end = today - dt.timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def parse_args() -> argparse.Namespace:
    default_start, default_end = default_dates()
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=default_start)
    parser.add_argument("--end", default=default_end)
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--pick-time", default="14:45")
    parser.add_argument("--cost-pct", type=float, default=0.15)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--kline-days", type=int, default=90)
    parser.add_argument("--max-detail", type=int, default=0, help="0 means analyze every relaxed prefilter candidate")
    parser.add_argument("--max-codes", type=int, default=0, help="debug only; 0 means full current A-share universe")
    parser.add_argument("--refresh-cache", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        trade_path, daily_path, report_path = run_backtest(args)
        print(f"BACKTEST_TRADES={trade_path}")
        print(f"BACKTEST_DAILY={daily_path}")
        print(f"BACKTEST_REPORT={report_path}")
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
