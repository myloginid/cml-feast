"""Create the HMS schema `manishm` and populate it with Berka banking tables."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from feast_demo import build_spark_session
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp

PROJECT_ROOT = Path(__file__).resolve().parent
BERKA_DIR = PROJECT_ROOT / "berka"
MANISHM_SCHEMA = "manishm"
BERKA_TABLES = [
    "account",
    "card",
    "client",
    "disp",
    "district",
    "loan",
    "order",
    "trans",
]


def _date_transformer(column: str, fmt: str) -> Callable[[DataFrame], DataFrame]:
    def _apply(df: DataFrame) -> DataFrame:
        if column not in df.columns:
            return df
        return df.withColumn(column, to_date(col(column).cast("string"), fmt))

    return _apply


def _timestamp_transformer(column: str, fmt: str) -> Callable[[DataFrame], DataFrame]:
    def _apply(df: DataFrame) -> DataFrame:
        if column not in df.columns:
            return df
        return df.withColumn(column, to_timestamp(col(column).cast("string"), fmt))

    return _apply


TABLE_TRANSFORMS: dict[str, Callable[[DataFrame], DataFrame]] = {
    "account": _date_transformer("date", "yyMMdd"),
    "loan": _date_transformer("date", "yyMMdd"),
    "trans": _date_transformer("date", "yyMMdd"),
    "card": _timestamp_transformer("issued", "yyMMdd HH:mm:ss"),
}


def _load_csv_table(spark: SparkSession, table_name: str) -> DataFrame:
    csv_path = BERKA_DIR / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Berka CSV not found: {csv_path}")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("quote", '"')
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("timestampFormat", "yyMMdd HH:mm:ss")
        .load(str(csv_path))
    )
    transformer = TABLE_TRANSFORMS.get(table_name)
    if transformer is not None:
        df = transformer(df)
    return df


def create_manishm_tables() -> None:
    spark = build_spark_session()
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {MANISHM_SCHEMA}")
        for table in BERKA_TABLES:
            df = _load_csv_table(spark, table)
            df.write.mode("overwrite").format("parquet").saveAsTable(
                f"{MANISHM_SCHEMA}.{table}"
            )
            row_count = df.count()
            print(
                f"Loaded {row_count} rows into table {MANISHM_SCHEMA}.{table} "
                f"from {BERKA_DIR / f'{table}.csv'}"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    create_manishm_tables()
