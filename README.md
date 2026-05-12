# funding-top10

Daily Slack report: top 10 BINANCE-U funding-rate symbols (high mean, low std over the past 7 days) plus the list of biyi `LONGSHORT_BINANCE*` tickers traded with `max_position > 1000` USD in the last 24h. Tickers that are in *both* lists are flagged with 🟨 in the Top 10 table.

## What it does

1. Query `funding_rate_data_original` + `open_interest_data_original` + `risk_manager_sys_param` (BINANCE-U only) to get per-symbol latest funding rate, 7-day mean/std, open interest, and MMR haircut.
2. Rank: top 50 by `mean_7d_funding_rate` desc → top 10 of those by `std_7d_funding_rate` asc.
3. Query `biyi_strategy_data_his` for distinct tickers with strategy `LONGSHORT_BINANCE*` and `max_position_in_usd > 1000` in the last 24h.
4. Build a Slack message: Top 10 table + biyi ticker list. Top-10 rows whose `base/quote` ticker is in the biyi list are prefixed with 🟨.
5. POST to a Slack incoming webhook.

Designed to run daily at 08:00 Beijing time on a Windows machine via Task Scheduler — see `deploy/README.md`.

## Project layout

```
funding-rate-daily-top10/
├── src/funding_top10/
│   ├── config.py          config.yaml loader (qijia DB fields + slack)
│   ├── queries.py         the two SQLs
│   ├── scoring.py         top50-by-mean → top10-by-std
│   ├── slack_message.py   build + post the message
│   └── main.py            entry point: fetch → rank → post
├── tests/                 unit tests (config + scoring + slack_message)
├── deploy/                Windows Task Scheduler bat + README
├── requirements.txt
├── pyproject.toml
└── config.yaml.example
```

## Dev setup (mac/Linux)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .             # makes `python -m funding_top10.main` work
pip install pytest           # for unit tests
pytest                        # runs config + scoring + slack_message tests
```

`pip install -e .` is required because the source lives under `src/`. Without it
you'd have to set `PYTHONPATH=src` manually each time you run.

## Running locally (with a real DB)

```bash
cp config.yaml.example config.yaml
# edit config.yaml — fill in qijia.host/port/user/password/database and slack.webhook
python -m funding_top10.main
```

## Configuration

Everything lives in `config.yaml` (git-ignored — use `config.yaml.example` as a template).

```yaml
qijia:
  host: ""           # required
  port: 5432         # required
  user: ""           # required
  password: ""       # required — URL-encoded automatically when building the DSN
  database: ""       # required

slack:
  webhook: ""        # required — Slack incoming webhook URL
  channel: ""        # optional — only needed if you later switch to files.upload
```

The qijia DSN is constructed at runtime as `postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DATABASE`. User and password are URL-encoded so special characters (`@`, `/`, `#`, etc.) are safe.

## Tweaking the SQL

- The "top X by mean" cut is 50 by default; "top N final" is 10. Both are kwargs on `scoring.select_top10()` and can be changed in `main.py`.
- The biyi lookback window is "1 day" via `biyi_tickers_sql(lookback_interval)`. Change in `main.py` if needed.
- BINANCE-U restriction is hardcoded in `queries.py`. To re-enable other exchanges, edit the `exchange = 'BINANCE-U'` filters in the CTEs.

## Deployment

See `deploy/README.md`.
