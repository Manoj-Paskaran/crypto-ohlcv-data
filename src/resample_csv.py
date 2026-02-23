from pathlib import Path

import pandas as pd

INPUT_CSV = Path("data/btc/btc_usdt_1min.csv")
OUTPUT_DIR = Path("data/btc/")

TIMEFRAMES = ["1min", "5min", "15min", "60min", "1D"]


def resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    return (
        df.set_index("time")
        .resample(timeframe)
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna()
        .reset_index()
    )


def write_resampled_csv(input_csv: Path, output_dir: Path, timeframes: list[str]):
    print(f"Reading {input_csv}...")
    df = pd.read_csv(input_csv, parse_dates=["time"])
    print(f"Read {len(df)} rows from {input_csv}.")

    for timeframe in timeframes:
        resampled_df = resample_ohlcv(df, timeframe)
        output_csv = output_dir / f"{input_csv.stem.replace('_1min', '')}_{timeframe}.csv"
        resampled_df.to_csv(output_csv, index=False)
        resampled_df.to_parquet(output_csv.with_suffix(".parquet"), index=False)
        print(
            f"Written resampled data to {output_csv} and {output_csv.with_suffix('.parquet')}"
        )


def main():
    write_resampled_csv(INPUT_CSV, OUTPUT_DIR, TIMEFRAMES)


if __name__ == "__main__":
    main()
