import logging
from datetime import datetime, timezone

import ccxt
import pandas as pd
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


def to_ms(date_str):
    return int(
        datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc).timestamp() * 1000
    )


def fetch_ohlcv(exchange_id, symbol, timeframe, start, end, output_csv):
    exchange: ccxt.Exchange = getattr(ccxt, exchange_id)(
        {
            "enableRateLimit": True,
        }
    )
    exchange.load_markets()

    since = to_ms(start)
    end_ms = to_ms(end)

    LIMIT = 1000  # max for most exchanges
    rows = []

    @retry(
        retry=(retry_if_exception_type(Exception)),
        stop=stop_after_attempt(8),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def fetch_ohlcv_retry(
        exchange: ccxt.Exchange, symbol: str, timeframe: str, since: int, limit: int
    ):
        return exchange.fetch_ohlcv(symbol, timeframe, since, limit)

    while since < end_ms:
        print(
            f"Fetching data since {datetime.fromtimestamp(since / 1000, tz=timezone.utc)}..."
        )

        try:
            ohlcv = fetch_ohlcv_retry(exchange, symbol, timeframe, since, LIMIT)

            if not ohlcv:
                break

            rows.extend(ohlcv)
            since = ohlcv[-1][0] + 1  # move to the next timestamp

        except Exception as e:
            print(f"Error fetching data (giving up after retries): {e}")

    df = pd.DataFrame(
        rows, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df = df.assign(time=pd.to_datetime(df["timestamp"], unit="ms", utc=True))[
        ["time", "open", "high", "low", "close", "volume"]
    ]

    df.to_csv(output_csv, index=False)

    return df


if __name__ == "__main__":
    # test = fetch_ohlcv(
    #     exchange_id="binance",
    #     symbol="BTC/USDT",
    #     timeframe="1m",
    #     start=datetime(2015, 1, 1).isoformat(),
    #     end=datetime.now().isoformat(),
    #     output_csv="btc_usdt_1m.csv",
    # )

    eth = fetch_ohlcv(
        exchange_id="binance",
        symbol="ETH/USDT",
        timeframe="1m",
        start=datetime(2015, 1, 1).isoformat(),
        end=datetime.now().isoformat(),
        output_csv="eth_usdt_1m.csv",
    )