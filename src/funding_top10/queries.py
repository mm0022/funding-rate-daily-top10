"""SQL queries for the funding top10 daily report.

Two queries:
  - funding_stats_sql:  per-symbol latest funding, 3d/7d mean & std, open interest,
                        MMR haircut. BINANCE-U only.
  - biyi_tickers_sql:   distinct tickers (e.g. "ENA/USDT") that have been touched
                        by a LONGSHORT_BINANCE* strategy with position > 1000 USD
                        in the recent window.
"""

FUNDING_STATS_SQL = """
-- 当前open_interest
with open_interest as
(
select exchange,symbol,base,"quote" ,settle, "timestamp",open_interest_value
from open_interest_data_original
where (exchange,symbol,base,"quote" ,settle, "timestamp")
in
(
select  exchange,symbol,base,"quote" ,settle,max("timestamp") as recent_timestamp
from open_interest_data_original
group by exchange,symbol,base,"quote" ,settle
)
)
-- funding rate 信息
-- 最新funding
, recent_funding as
(
select exchange,symbol,base,"quote" ,settle, "timestamp",funding_rate
from funding_rate_data_original
where ("timestamp")
in
(
select  max("timestamp") as recent_timestamp
from funding_rate_data_original
)
-- BINANCE-U only (per project scope)
and exchange = 'BINANCE-U'
)
-- 过去3天 funding 总额（不取均值——这样跟 funding 周期无关）
, past_3day as
(
select exchange,symbol,base,"quote" ,settle,
sum(funding_rate) as sum_3d_funding_rate
from funding_rate_data_original
where "timestamp" >
(
select  max("timestamp") - 3*60*60*24*1000 as recent_timestamp
from funding_rate_data_original
)
and exchange = 'BINANCE-U'
group by
exchange,symbol,base,"quote" ,settle
)
, past_7day as
(
-- 过去7天 funding 总额 + per-event std（注：std 量纲随 funding 周期变化，仅作参考）
select exchange,symbol,base,"quote" ,settle,
sum(funding_rate) as sum_7d_funding_rate,
stddev(funding_rate) as std_7d_funding_rate
from funding_rate_data_original
where "timestamp" >
(
select  max("timestamp") - 7*60*60*24*1000 as recent_timestamp
from funding_rate_data_original
)
and exchange = 'BINANCE-U'
group by
exchange,symbol,base,"quote" ,settle
)
, funding_stats as
(
select recent_funding.*,
sum_3d_funding_rate,
sum_7d_funding_rate,
std_7d_funding_rate
from recent_funding
left join past_3day on recent_funding.exchange = past_3day.exchange
and recent_funding.symbol = past_3day.symbol
left join past_7day on recent_funding.exchange = past_7day.exchange
and recent_funding.symbol = past_7day.symbol
)
, MMR_haircut as (
select
case when exchange = 'OKEX' then 'OKX' else exchange end as exchange,
symbol,
value as haircut
from risk_manager_sys_param
where region = 'weight'
and ((exchange in ('OKEX','BINANCE') and account_type  = 'ALL_CROSSED')
or (exchange = 'BYBIT' and account_type = 'multi_asset'))
and (step_idx = '0' or step_idx is null)
)
select
funding_stats.*,
open_interest.open_interest_value,
MMR_haircut.haircut
from funding_stats
left join open_interest on funding_stats.exchange = open_interest.exchange
and funding_stats.symbol = open_interest.symbol
left join MMR_haircut on MMR_haircut.symbol = funding_stats.base
and split_part(funding_stats.exchange,'-',1) = MMR_haircut.exchange
;
"""


def biyi_tickers_sql(lookback_interval: str = "1 day") -> str:
    """Build the biyi tickers query with a dynamic lookback window.

    Args:
        lookback_interval: a PostgreSQL INTERVAL literal, e.g. "1 day", "12 hours".

    Returns:
        SQL string suitable for sqlalchemy.text(). The "%" in LIKE is literal here
        because sqlalchemy text() does not perform DBAPI paramstyle substitution.
    """
    return f"""
        select distinct(ticker) as ticker
        from biyi_strategy_data_his s
        where s.sample_time > NOW() - INTERVAL '{lookback_interval}'
          and s.strategy_name like 'LONGSHORT_BINANCE%'
          and s.max_position_in_usd > 1000;
    """
