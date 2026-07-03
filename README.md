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

## Important Boundary

This project is a research and screening tool only. It does not place orders and does not provide guaranteed returns. Always verify market data, announcements, liquidity, and risk in your trading software before making decisions.
