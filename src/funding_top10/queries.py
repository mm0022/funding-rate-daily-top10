"""SQL queries for the funding top10 daily report.

After the 2026-05-13 migration the funding/OI/haircut data is fetched live
from Binance APIs (see ``binance_api.py``). The only remaining SQL is the
biyi tickers query against the internal qijia DB.
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
