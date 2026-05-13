# Windows deployment

## 1. Install

Open a regular `cmd` window:

```cmd
git clone <repo-url> C:\funding-top10
cd C:\funding-top10
python -m venv .venv
.venv\Scripts\activate.bat
pip install -r requirements.txt
pip install -e .

:: Install the internal nexus_data_hub_sdk from the alpha repo's vendor dir.
:: Adjust the path to wherever alpha is checked out on this machine.
pip install C:\path\to\alpha\vendor\nexus-data-hub-sdk

copy config.yaml.example config.yaml
notepad config.yaml
```

> `pip install -e .` installs this project itself into the venv so that
> `python -m funding_top10.main` can find the package without needing PYTHONPATH.
> `run_daily.bat` also sets `PYTHONPATH=src` as a belt-and-suspenders fallback,
> so the bat works even if you forgot `pip install -e .`.

In `config.yaml`, fill in:
- `qijia.host / port / user / password / database` — Postgres connection fields
- `slack.webhook` — Slack incoming webhook URL
- `slack.channel` — optional, leave empty unless you switch to file-upload mode later
- `datahub.prefix / api_key / gateway_url` — for haircut data (defaults in config.yaml.example are tyo-prod)
- `proxy` — corp proxy URL; leave empty if not needed

## 2. Smoke-run once manually

```cmd
deploy\run_daily.bat
type logs\daily_*.log
```

Confirm a message arrived in the Slack channel and that the log shows no errors.

## 3. Register with Task Scheduler

Open an **elevated** `cmd` (Run as administrator), then:

```cmd
schtasks /Create ^
  /TN "FundingTop10Daily" ^
  /SC DAILY ^
  /ST 08:00 ^
  /TR "C:\funding-top10\deploy\run_daily.bat" ^
  /RL HIGHEST ^
  /F
```

Adjust `/TR` to the actual install path.

### Timezone note

The trigger fires at **08:00 Windows local time**. If this machine's system timezone is *China Standard Time (UTC+8)* the report fires at Beijing 08:00 as intended. If the machine is on a different timezone, either:

- change the system timezone (Settings → Time & language → Date & time), or
- change `/ST` to the equivalent local time of 08:00 Beijing (e.g. UTC machine → `/ST 00:00`).

## 4. Manage

```cmd
:: status / next run time
schtasks /Query /TN "FundingTop10Daily" /V /FO LIST

:: run now (for testing)
schtasks /Run /TN "FundingTop10Daily"

:: remove
schtasks /Delete /TN "FundingTop10Daily" /F
```

## 5. Logs

Each run appends to `logs\daily_YYYYMMDD.log` (date in machine local time, yyyyMMdd format). Tail by date:

```cmd
type logs\daily_20260511.log
```

If a run fails, the log will show the Python traceback and Task Scheduler will mark the task as completed with a non-zero exit code (visible in `schtasks /Query ... /V`).
