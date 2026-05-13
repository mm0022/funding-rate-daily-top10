# funding-top10

Daily Slack report: top 10 BINANCE-U funding-rate symbols (high mean, low std over the past 7 days) plus the list of biyi `LONGSHORT_BINANCE*` tickers traded with `max_position > 1000` USD in the last 24h. Tickers that are in *both* lists are flagged with 🟨 in the Top 10 table.

## What it does

1. Fetch from Binance public/signed APIs (USDT-quoted BINANCE-U perps only):
   - latest funding rate + mark price (`/fapi/v1/premiumIndex`)
   - past 7 days of funding events per symbol (`/fapi/v1/fundingRate`)
   - current open interest per symbol (`/fapi/v1/openInterest`)
   - per-asset collateral rate / "haircut" (`/sapi/v1/portfolio/collateralRate`, **signed**)
2. Compute per-symbol `sum_3d_funding_rate`, `sum_7d_funding_rate`, `std_7d_funding_rate` from history; convert OI to USD via mark price.
3. Rank: top 50 by `sum_7d_funding_rate` desc → top 10 of those by `std_7d_funding_rate` asc (haircut must be >= 0.5).
4. Query `biyi_strategy_data_his` (qijia DB) for distinct tickers with strategy `LONGSHORT_BINANCE*` and `max_position_in_usd > 1000` in the last 24h.
5. Build a Slack message: merged Top 10 ∪ biyi rows, sorted by `sum_7d_funding_rate` desc. Biyi rows are prefixed with 🔴.
6. POST to a Slack incoming webhook.

Designed to run daily at 08:00 Beijing time on a Windows machine via Task Scheduler — see `deploy/README.md`.

## Project layout

```
funding-rate-daily-top10/
├── src/funding_top10/
│   ├── config.py          config.yaml loader (qijia DB + slack + binance keys)
│   ├── binance_api.py     async client for fapi/sapi (funding/OI/haircut)
│   ├── queries.py         the biyi SQL (only DB query left)
│   ├── scoring.py         top50-by-sum-7d → top10-by-std-7d
│   ├── slack_message.py   build + post the message
│   └── main.py            entry point: fetch (API + DB) → rank → post
├── tests/                 unit tests
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
  host: ""           # required (only used for the biyi tickers query)
  port: 5432         # required
  user: ""           # required
  password: ""       # required — URL-encoded automatically when building the DSN
  database: ""       # required

slack:
  webhook: ""        # required — Slack incoming webhook URL
  channel: ""        # optional — only needed if you later switch to files.upload

binance:
  api_key: ""        # optional — only needed for the signed /sapi/v1/portfolio/collateralRate
  api_secret: ""     # endpoint that supplies the "haircut" column. Without keys,
                     # haircut will be NaN; everything else still works.
```

The qijia DSN is constructed at runtime as `postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DATABASE`. User and password are URL-encoded so special characters (`@`, `/`, `#`, etc.) are safe.

## Tweaking the SQL

- The "top X by mean" cut is 50 by default; "top N final" is 10. Both are kwargs on `scoring.select_top10()` and can be changed in `main.py`.
- The biyi lookback window is "1 day" via `biyi_tickers_sql(lookback_interval)`. Change in `main.py` if needed.
- BINANCE-U restriction is hardcoded in `queries.py`. To re-enable other exchanges, edit the `exchange = 'BINANCE-U'` filters in the CTEs.

## Deployment

See `deploy/README.md`.
