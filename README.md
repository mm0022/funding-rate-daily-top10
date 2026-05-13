# funding-top10

每日 Slack 报告：BINANCE-U USDT 永续合约里，按"置信下界年化收益"打分排前 N 的标的，并强制并入 biyi LONGSHORT 策略当前持仓的所有币。报告每天 08:00 北京时间通过 Windows Task Scheduler 触发，详见 `deploy/README.md`。

## 数据来源

| 数据 | 来源 | 说明 |
|------|------|------|
| funding rate / mark price | Binance `GET /fapi/v1/premiumIndex` | 当前 funding |
| 历史 7 天 funding 事件 | Binance `GET /fapi/v1/fundingRate` | 算 `sum_3d` / `sum_7d` / `std_7d` |
| funding 周期（1h/4h/8h） | Binance `GET /fapi/v1/fundingInfo` | 用于 std 年化 |
| open interest | Binance `GET /fapi/v1/openInterest` | × mark 转 USD |
| haircut（折扣率） | DataHub (`nexus_data_hub_sdk`) | 内网网关；并行 10 worker 抓 |
| biyi 持仓 | `POST https://biyi.tky.laozi.pro/biyi/api/strategies/list` | 内网无 auth；query 见下 |

## 处理流程

```
                    ┌─ Binance fapi ──→ funding_df  (funding / OI / std_7d / interval)
                    │
main.py  ─────────┬─┤
                  │ ├─ biyi /strategies/list ──→ positions  (ticker, position_usd)
                  │ │
                  │ └─ DataHub ──→ haircut[base]
                  │
                  ▼
            scoring.select_rows_to_show
                  │   1. std_7d 按 funding 周期年化:  std × sqrt((24/h) × 365)
                  │   2. 硬过滤:  haircut ≥ min_haircut  且  OI ≥ min_oi_usd
                  │   3. score = annualized_apr − z × annualized_std
                  │            annualized_apr = sum_7d × 365 / 7
                  │      z=1.645 ⇔ 单侧 95% 置信下界（"该置信度下年化至少能赚到这么多"）
                  │   4. 按 score 降序取 top N
                  │   5. 把 biyi 的 ticker 强制并入（不论排名 / 是否过硬过滤）
                  ▼
            slack_message.build_message
                  │   一张表，按 score 降序
                  │   biyi 行用 🔴 标记，并多两列: pos（持仓 USD） / pct%（占 biyi 总仓位）
                  ▼
            slack_message.post_to_slack  → Slack incoming webhook
```

## 评分细节

打分公式只用 funding 数据：

```
score = (sum_7d_funding_rate × 365 / 7) − z × std_7d_annualized
```

- **不包含 haircut / OI**：这两个是硬过滤，过不了就出局，不在 score 里"换分"
- **z 可配**：`score.confidence_z` — 1.0 ≈ 84%, 1.645 ≈ 95% (默认), 2.0 ≈ 97.7%
- **biyi 并入**：biyi 持有的币即使没过 haircut/OI 硬过滤、即使 score 排不上，也会出现在表里（用于持续监控自己的仓位）

## 项目结构

```
funding-rate-daily-top10/
├── src/funding_top10/
│   ├── config.py          load_config()，YAML → 各 dataclass
│   ├── binance_api.py     async httpx 抓 fapi（funding / OI / interval）
│   ├── biyi_api.py        POST /strategies/list → [{ticker, position_usd}]
│   ├── datahub.py         DataHub SDK 封装 + Windows AV 兼容补丁 + 并行抓 haircut
│   ├── scoring.py         std 年化、硬过滤、置信下界打分、biyi 并入
│   ├── slack_message.py   表格渲染 + webhook POST（含重试）
│   ├── queries.py         （历史遗留；qijia 已废弃，文件已不参与主流程）
│   └── main.py            入口：load_config → 拉数 → 排序 → 发 Slack
├── tests/                 单元测试（pytest）
├── deploy/                run_daily.bat + 部署说明
├── scripts/               临时调试脚本（不进主流程）
├── config.yaml.example    复制为 config.yaml 后填值；config.yaml 是 git-ignored
└── pyproject.toml
```

## 配置（config.yaml）

```yaml
biyi:
  base_url: "https://biyi.tky.laozi.pro/biyi/api"
  # query 在 server 端做账户和最小持仓过滤。
  # productType 部分代码会自动追加，这里不用写。
  query: "$accountMap like XXX and $maxPositionQty gt 10"

slack:
  webhook: ""              # 必填，Slack incoming webhook
  channel: ""              # 可选，webhook 模式下未用

datahub:
  prefix: "CYBERX_PROD"    # haircut 命名空间
  api_key: "..."
  gateway_url: "https://nexus.tyo.cyberx.com/nexus-data-hub-gateway/"
  cache_dir: ""            # 空 = ~/.datahub_cache；Windows AV 撞写权限就指到可写路径

filters:
  min_haircut: 0.5         # < 此值的币丢掉（NaN 视为 0）
  min_oi_usd: 5000000      # OI < 5M USD 丢掉

score:
  confidence_z: 1.645      # 一侧 z；越大越保守

proxy: ""                  # Binance + Slack 走代理；DataHub 与 biyi（内网）不走

# 以下为兼容旧 config 的占位，不影响逻辑
qijia: { ... }             # 已废弃（biyi 走 API 了）
score_weights: { ... }     # 已废弃（score 已改成置信下界）
```

## 本地开发

```bash
./bootstrap.sh           # 或手动: python -m venv .venv && pip install -r requirements.txt && pip install -e .
source .venv/bin/activate
pytest                   # 单测
cp config.yaml.example config.yaml
# 填值
python -m funding_top10.main
```

## 部署

Windows Task Scheduler 每天 08:00 北京时间触发 `deploy/run_daily.bat`，细节看 `deploy/README.md`。
