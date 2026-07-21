"""
About this file:
1. The transform.py file cleans and engineers features from the validated raw DataFrame produced by validate.py
2. Duplicates records are removed here
3. Date is parsed in datetime for timeseries modelling
4. Normalization is done here
5. Cross-market price spreads,rolling statistics and lag features are engineered here
6. INPUT: A validated pandas DataFrame from validate.py with 1594 rows and 16 columns
7. OUTPUT: A cleaned, featured-engineered DataFrame ready to be partitioned and for federated learning
8. A parquet file will be generated and saved as a timestamped at data/processed/..
9. To execute, run this: python -m pipeline.transform
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as numpy
import pandas as pd
from prefect import task, get_run_logger

from pipeline.config import(
    TARGET_MARKETS,
    TARGET_COMMODITIES,
    PROCESSED_DIR,
)

from itertools import combinations

##=== Builder Internal Helpers ===#
def _ensure_dir(path: Path) -> Path:
    """ Creates a directory at path if it does not exist"""
    path.mkdir(parents=True, exist_ok=True)
    return path

def _timestamped_path(prefix: str, ext: str = "parquet") -> Path:
    """Build a unique timestamped output path inside data/processed/. that uses 
    parquet instead of CSV"."""
    time_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{time_stamp}.{ext}"
    return _ensure_dir(PROCESSED_DIR) / filename

def _remove_duplicates(data_frame: pd.DataFrame, logger) -> pd.DataFrame:
    """Remove rows with identical date, market name, commodity and price with
    the exception for the first row"""
    before = len(data_frame)
    data_frame = data_frame.drop_duplicates(
        subset=["date", "market", "commodity"], keep="first",
    ).reset_index(drop=True)

    dropped_duplicates = before - len(data_frame)
    logger.info(
        f"_remove_duplicates: {before:,} -> "
        f"{len(data_frame):,} rows "
        f"({dropped_duplicates:,} duplicates removed)"
    )
    return data_frame

def _parse_dates(data_frame: pd.DataFrame, logger) -> pd.DataFrame:
    """ This function parses the date column from string to datetime and
    sort the DataFrame chronologically within each market-commodity group.
    It returns a data frame with parsed datetime date column, sorted by market,
    commodity, and date.
    """
    initial_data_type = data_frame["date"].dtype 
    data_frame["date"] = pd.to_datetime(data_frame["date"], format="%Y-%m-%d")
    data_frame = data_frame.sort_values(
        by=["market", "commodity", "date"]
    ).reset_index(drop=True)
    logger.info(
        f"_parse_dates: date column"
        f"{initial_data_type} -> {data_frame['date'].dtype} |"
        f"data sorted by market, commodity, and date"
    )
    return data_frame

def _normalised_prices(data_frame: pd.DataFrame, logger) -> pd.DataFrame:
    """
    This functions normalise all prices to naira per kilogram. A new column 'price_per_kg' is added
    to the this DataFrame. The price in dollar is also normalised to dollar per KG 
    """
    ## Create a dictionary of each unit and their divisors ##
    unit_divisors = {
        "100 KG": 100.0,
        "50 KG": 50.0,
        "2.8 KG": 2.8,
        "2.6 KG": 2.6,
        "KG": 1.0,
    }

    ## Map each distinct unit to its divisor ##
    divisors = data_frame["unit"].map(unit_divisors)

    ## Detect unmapped units before dividing ##
    unmapped = data_frame.loc[divisors.isna(), "unit"].unique().tolist()
    if unmapped:
        logger.warning(
            f"_normalise_prices: unmapped units"
            f"found - {unmapped}. "
            f"The price_per_kg in these rows will be missing."
        )
    ## Compute the normalised price columns ##
    data_frame["price_per_kg"] = data_frame["price"] / divisors
    data_frame["usdprice_per_kg"] = data_frame["usdprice"] / divisors

    data_frame["price_per_kg"] = data_frame["price_per_kg"].round(3)
    data_frame["usdprice_per_kg"] = data_frame["usdprice_per_kg"].round(6)

    logger.info(
        f"_normalise_prices: price_per_kg range = "
        f"[{data_frame['price_per_kg'].min():.2f}, "
        f"{data_frame['price_per_kg'].max():.2f}] NGN/KG"
    )
    return data_frame

def _lag_features(
    data_frame: pd.DataFrame,
    logger,
    lags: list[int] = [1, 3, 6, 12],
    ) -> pd.DataFrame:

    group_columns = ["market", "commodity"]
    for lag in lags:
        col_name = f"price_per_kg_lag_{lag}m"
        data_frame[col_name] = (
            data_frame.groupby(group_columns)["price_per_kg"].shift(lag)
        )
    ## detect the numbers of null values each lag produced ##
    lag_cols = [f"price_per_kg_lag_{n}m" for n in lags]
    null_counts = {col: int(data_frame[col].isna().sum()) for col in lag_cols}

    logger.info(
        f"_engineer_lag_features: added lags"
        f"{lags} months | Null counts: {null_counts}"
    )
    return data_frame

def _rolling_stats(data_frame: pd.DataFrame,logger,windows: list[int]=[3,6]):
    """This function produces the rolling mean and standard deviation of
    price_per_kg within each market-commodity group.
    Here, two additional columns are added:
    1. price_per_kg_roll_mean where Rolling MEAN captures price trend
    2. price_per_roll_std where Rolling STD captures price volatility
    3. The functions returns a DataFrame with rolling stats (MEAN and STD) columns added.
    """
    group_cols = ["market", "commodity"]

    for window in windows:
        mean_column = f"price_per_kg_roll_mean_{window}m"
        std_column = f"price_per_kg_roll_std_{window}m"

        grouped_col = data_frame.groupby(group_cols)["price_per_kg"]

        data_frame[mean_column] = grouped_col.transform(
            lambda x: x.rolling(
                window=window,
                min_periods=1,
            ).mean().round(3)
        )
        data_frame[std_column] = grouped_col.transform(
            lambda x: x.rolling(
                window=window,
                min_periods=1,
            ).std().round(3)
        )

    ## Range of rolling means logged to enhance proper check ##
    cols_added = []
    for window in windows:
        cols_added.extend([
            f"price_per_kg_roll_mean_{window}m",
            f"price_per_kg_roll_std_{window}m",
        ])

    logger.info(
        f"_engineer_rolling_stats: added "
        f"{len(cols_added)} columns for "
        f"windows {windows} months"
    )
    return data_frame

def _engineer_cross_market_spreads(
    data_frame: pd.DataFrame,
    logger,
) -> pd.DataFrame:
    """
    Engineer cross-market price spread features
    for every commodity and market pair.

    Collects all spread DataFrames first, then
    merges once — avoids column conflicts from
    multiple sequential merges on date.
    """
    from itertools import combinations

    spread_cols_added = []
    all_spreads = []

    for commodity in TARGET_COMMODITIES:
        comm_df = data_frame[
            data_frame["commodity"] == commodity
        ][["date", "market", "price_per_kg"]].copy()

        pivoted = comm_df.pivot_table(
            index="date",
            columns="market",
            values="price_per_kg",
            aggfunc="mean",
        ).reset_index()

        markets_in_data = [
            m for m in TARGET_MARKETS
            if m in pivoted.columns
        ]

        spread_df = pivoted[["date"]].copy()

        for mkt_a, mkt_b in combinations(markets_in_data, 2):
            col_name = f"spread_{mkt_a}_{mkt_b}_{commodity}"
            spread_df[col_name] = (
                pivoted[mkt_a] - pivoted[mkt_b]
            ).round(3)
            spread_cols_added.append(col_name)

        all_spreads.append(spread_df)

    # Merge all spread DataFrames on date in one step
    if all_spreads:
        from functools import reduce
        spreads_combined = reduce(
            lambda left, right: pd.merge(
                left, right, on="date", how="outer"
            ),
            all_spreads,
        )
        data_frame = data_frame.merge(
            spreads_combined,
            on="date",
            how="left",
        )

    logger.info(
        f"_engineer_cross_market_spreads: "
        f"added {len(spread_cols_added)} spread columns"
    )
    logger.info(f"  Columns: {spread_cols_added}")

    return data_frame

@task(
    name="transform-wfp-nigeria-data",
    retries=2,
    retry_delay_seconds=10,
    description=(
        "Clean and engineer features from validated WFP cereals DataFrame"
        "Save timestamped Parquet to data/processed/."

    ),
)
def transform_data(data_frame: pd.DataFrame) -> Path:
    """
    This function is the entry point for this module and removes duplicates;
    Parse dates and sort; normalise price to NGN per KG; Engineer lag features (1,3,6,12 months);
    Engineer rolling stats (3 and 6 month windows); Engineer cross-markets price spreads
    """
    try:
        logger = get_run_logger()
    except Exception:
        logger = logging.getLogger(__name__)
    logger.info("== Data Transformation Started ==")
    logger.info(
        f"Input: {len(data_frame):,} rows *"
        f"{len(data_frame.columns)} columns"
    )

    data_frame = _remove_duplicates(data_frame, logger) ## remove duplicates

    data_frame = _parse_dates(data_frame, logger) ## parse dates 

    data_frame = _normalised_prices(data_frame, logger) ## normalise prices to naira/kg

    data_frame = _lag_features(data_frame, logger) 

    data_frame = _rolling_stats(data_frame, logger)

    data_frame = _engineer_cross_market_spreads(data_frame, logger)

    ## Check that the output is not empty ##
    if data_frame.empty:
        raise ValueError(
            "Transformed dataframe is empty. Check the extraction and validation stage."
        )
    ## --- Save to Parquet --- ##
    result_path = _timestamped_path("wfp_nga_cereals")
    data_frame.to_parquet(result_path, index=False)

    logger.info(
        f"Saved {len(data_frame):,} rows *"
        f"{len(data_frame.columns)} columns"
        f"{result_path.name}"
    )
    logger.info(
        f"New columns added: "
        f"{[c for c in data_frame.columns if c not in ['date','admin1','admin2','market','market_id','latitude','longitude','category','commodity','commodity_id','unit','priceflag','pricetype','currency','price','usdprice']]}"
    )
    logger.info("=== Completed Data Transformation ===")
    return result_path

### Build the standalone runner that runs the most recent validated file 
### automatically.

if __name__ == "__main__":
    import sys
    from pipeline.validate import validate_data 

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger(__name__)

    ## Trace the most recent raw file ##
    raw_files = sorted(Path("data/raw").glob("*.csv"))
    if not raw_files:
        log.error(
            "No raw CSV files found in data/raw/. Run the extract file first"
        )
        sys.exit(1)
    latest_raw = raw_files[-1]
    log.info(f"Input file: {latest_raw.name}")

    log.info("Running validation first...")
    validated_data_frame = validate_data.fn(latest_raw)
    log.info("___Running transformation___")
    output_path = transform_data.fn(validated_data_frame)

    data_frame_result = pd.read_parquet(output_path)

    print("\n" + "="*45)
    print("TRANSFORMATION SUMMARY")
    print("=" * 45)
    print(f"Input rows : {len(validated_data_frame):,}")
    print(f"Output rows : {len(data_frame_result):,}")
    print(
        f"Rows removed : "
        f"{len(validated_data_frame) - len(data_frame_result)}"
        f"(duplicates)"
    )
    print(f"Output columns : {len(data_frame_result.columns)}")
    print(f"Output file : {output_path}")
    print("="*45)
    print("Columns in output: ")
    for col in data_frame_result.columns:
        print(f"{col}")
    print("=" * 45)
    print("Sample row (first row, Ibadan Maize):")
    Ibadan = data_frame_result[
        (data_frame_result["market"] == "Ibadan") &
        (data_frame_result["commodity"] == "Maize (white)")
    ]
    if not Ibadan.empty:
        print(Ibadan.iloc[0].to_string())
    print("=" * 45)