# daily_analysis

A-share tail-session stock picker and overnight strategy tracker.

This repository contains a GitHub Actions based version of the tail-session stock picker. It runs on GitHub-hosted runners, so it does not depend on a local Mac being awake.

## What It Does

- Runs every China market weekday at 14:45 Asia/Shanghai.
- Fetches public A-share market data from Eastmoney endpoints.
- Scores candidates using market breadth, sector strength, trend, price-volume conditions, and intraday tail-session behavior.
- Writes CSV and Markdown reports under `a_share_tail_picker/outputs/`.
- Tracks next-day 10:00 performance when prior candidate files exist.
- Writes learning samples and conservative adaptive rules under `a_share_tail_picker/learning/`.
- Opens or comments on a GitHub Issue titled `A股尾盘选股日报` with the daily summary.

## Manual Run

In GitHub, open **Actions -> A Share Tail Picker -> Run workflow**.

Locally:

```bash
python3 a_share_tail_picker/cloud_tail_picker.py daily --top 30 --max-detail 160 --workers 8
```

## Historical Backtest

Replay the current tail-session strategy over a historical window:

```bash
python3 a_share_tail_picker/backtest_tail_strategy.py --start 2026-06-01 --end 2026-07-02 --top 5 --workers 16
```

Backtest outputs are written under:

- `a_share_tail_picker/backtests/YYYY-MM-DD_to_YYYY-MM-DD/backtest_summary_*.md`
- `a_share_tail_picker/backtests/YYYY-MM-DD_to_YYYY-MM-DD/backtest_trades_*.csv`
- `a_share_tail_picker/backtests/YYYY-MM-DD_to_YYYY-MM-DD/backtest_daily_*.csv`

Backtest data uses the current active A-share universe, Tencent daily kline with Yahoo daily fallback, and Yahoo/Tencent 5-minute bars for candidate 14:45 snapshots and next-day 10:00 evaluation. Because public APIs do not provide a complete historical full-market 14:45 snapshot, turnover, amount, and volume ratio are approximate in backtests.

## Schedule

GitHub Actions cron uses UTC:

```yaml
cron: "45 6 * * 1-5"
```

That corresponds to 14:45 Asia/Shanghai.

## Outputs

- `a_share_tail_picker/outputs/YYYY-MM-DD/report_*.md`
- `a_share_tail_picker/outputs/YYYY-MM-DD/candidates_*.csv`
- `a_share_tail_picker/learning/learning_report_*.md`
- `a_share_tail_picker/learning/samples.csv`
- `a_share_tail_picker/learning/adaptive_rules.json`

## WeChat Notification

The recommended path is WeCom group bot webhook. GitHub Actions sends a Markdown notification to a WeCom group, and the message is visible on mobile WeChat/WeCom.

Steps:

1. Create or open a WeCom group.
2. Add a group bot and copy its webhook URL.
3. In this GitHub repository, open **Settings -> Secrets and variables -> Actions**.
4. Create a repository secret named:

```text
WECOM_WEBHOOK_URL
```

5. Paste the webhook URL as the secret value.
6. Run **Actions -> A Share Tail Picker -> Run workflow** once to test.

If `WECOM_WEBHOOK_URL` is not configured, the workflow will skip WeCom notification and still write GitHub Issues, artifacts, and repository reports.

## Important Boundary

This project is a research and screening tool only. It does not place orders and does not provide guaranteed returns. Always verify market data, announcements, liquidity, and risk in your trading software before making decisions.
