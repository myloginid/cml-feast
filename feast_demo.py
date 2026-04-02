"""Spark job for Great Expectations checks on the gx_demo_table and local Feast setup helpers."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import cml.data_v1 as cmldata
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    count,
    countDistinct,
    current_date,
    datediff,
    date_sub,
    floor,
    lit,
    least,
    max,
    min,
    row_number,
    sum,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql import Window

QUERY_SQL = "SELECT * FROM manishm.gx_demo_table"
CONNECTION_NAME = "go01-aw-dl"

MANISHM_SCHEMA = "manishm"

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--conf spark.yarn.principal=manishm@GO01-DEM.YLCU-ATMI.CLOUDERA.SITE "
    "--conf spark.yarn.keytab=/home/cdsw/go01-demo-aws-manishm.keytab pyspark-shell",
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Feast prep or Spark validation.")
    parser.add_argument(
        "--query",
        default=QUERY_SQL,
        help="Spark SQL query to run against the Hive catalog.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Optional JSON config that covers validator settings.",
    )
    parser.add_argument(
        "--berka-metrics",
        action="store_true",
        help=(
            "Compute the HMS tables `manishm.berka_customer_metrics`, "
            "`manishm.account_metrics`, and `manishm.loan_metrics` from the Berka dataset."
        ),
    )
    return parser.parse_args()


def load_config(config_path: Path | None) -> dict:
    if config_path is None:
        return {}
    with config_path.open("r", encoding="utf-8") as config_file:
        return json.load(config_file)


def build_spark_session() -> SparkSession:
    try:
        conn = cmldata.get_connection(CONNECTION_NAME)
        return conn.get_spark_session()
    except Exception:  # pragma: no cover - best-effort for local testing
        return (
            SparkSession.builder.appName("FeastDemo")
            .enableHiveSupport()
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .getOrCreate()
        )


def _parse_birth_date_expr(birth_number_col):
    birth_year = birth_number_col.substr(1, 2).cast("int")
    birth_year_full = when(birth_year >= 54, birth_year + 1900).otherwise(birth_year + 2000)
    birth_day = birth_number_col.substr(5, 2)
    birth_month = birth_number_col.substr(3, 2)
    birth_date = to_date(
        concat_ws("-", birth_year_full.cast("string"), birth_month, birth_day),
        "yyyy-MM-dd",
    )
    return birth_date


def _load_berka_tables(spark: SparkSession) -> dict[str, DataFrame]:
    account = (
        spark.table(f"{MANISHM_SCHEMA}.account")
        .withColumnRenamed("date", "opened_date")
        .withColumn("opened_date", to_date(col("opened_date").cast("string"), "yyMMdd"))
    )
    loan = (
        spark.table(f"{MANISHM_SCHEMA}.loan")
        .withColumnRenamed("date", "start_date")
        .withColumn("start_date", to_date(col("start_date").cast("string"), "yyMMdd"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("duration_months", col("duration").cast("double"))
        .withColumn("payments", col("payments").cast("double"))
    )
    trans = (
        spark.table(f"{MANISHM_SCHEMA}.trans")
        .withColumn("trans_date", to_date(col("date").cast("string"), "yyMMdd"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("balance", col("balance").cast("double"))
    )
    client = spark.table(f"{MANISHM_SCHEMA}.client").select("client_id", "birth_number")
    disp = spark.table(f"{MANISHM_SCHEMA}.disp").select("disp_id", "client_id", "account_id", "type")
    card = spark.table(f"{MANISHM_SCHEMA}.card").withColumn(
        "issued", to_timestamp(col("issued").cast("string"), "yyMMdd HH:mm:ss")
    )
    order = spark.table(f"{MANISHM_SCHEMA}.order").withColumn("amount", col("amount").cast("double"))

    return {
        "account": account,
        "loan": loan,
        "trans": trans,
        "client": client,
        "disp": disp,
        "card": card,
        "order": order,
    }


def _latest_daily_balance(trans_df: DataFrame) -> DataFrame:
    balance_window = Window.partitionBy("account_id", "trans_date").orderBy(col("trans_id").cast("long").desc())
    return (
        trans_df.withColumn("row_rank", row_number().over(balance_window))
        .filter(col("row_rank") == 1)
        .drop("row_rank")
    )


def _filter_last_n_days(df: DataFrame, date_col: str, feature_date, days: int) -> DataFrame:
    return df.filter(
        (col(date_col) >= date_sub(feature_date, days)) & (col(date_col) <= feature_date)
    )


def _build_berka_customer_metrics(spark: SparkSession, tables: dict[str, DataFrame] | None = None) -> DataFrame:
    tables = tables or _load_berka_tables(spark)
    feature_date = current_date()
    client_df = tables["client"]
    disp_df = tables["disp"]
    account_df = tables["account"]
    card_df = tables["card"]
    loan_df = tables["loan"]
    order_df = tables["order"]
    trans_df = tables["trans"]

    customer_accounts = disp_df.select("client_id", "account_id").distinct()
    account_first = (
        account_df.join(disp_df, "account_id")
        .groupBy("client_id")
        .agg(min("opened_date").alias("first_opened_date"))
    )

    disp_agg = (
        disp_df.groupBy("client_id").agg(
            countDistinct("account_id").alias("customer_num_accounts"),
            countDistinct(
                when(col("type") == "OWNER", col("account_id"))
            ).alias("customer_num_owner_accounts"),
            countDistinct(
                when(col("type") == "DISPONENT", col("account_id"))
            ).alias("customer_num_disponent_accounts"),
        )
    )

    card_join = disp_df.join(card_df, "disp_id", "left")
    card_agg = (
        card_join.groupBy("client_id").agg(
            max(
                when(col("card_id").isNotNull(), lit(1)).otherwise(lit(0))
            ).alias("customer_has_card_flag"),
            countDistinct("card_id").alias("customer_num_cards"),
            datediff(feature_date, max("issued")).alias("customer_card_recency_days"),
        )
    )

    daily_last_balance = _latest_daily_balance(trans_df)
    balance_30d = (
        _filter_last_n_days(daily_last_balance, "trans_date", feature_date, 30)
        .join(customer_accounts, "account_id")
        .groupBy("client_id")
        .agg(
            avg("balance").alias("customer_avg_account_balance_30d"),
            max("balance").alias("customer_max_account_balance_30d"),
        )
    )

    recent_trans = (
        _filter_last_n_days(trans_df, "trans_date", feature_date, 30)
        .join(customer_accounts, "account_id")
    )
    recent_txn_agg = (
        recent_trans.groupBy("client_id").agg(
            count("trans_id").alias("customer_total_txn_count_30d"),
            sum("amount").alias("customer_total_txn_amount_30d"),
            avg("amount").alias("customer_avg_txn_amount_30d"),
            sum(
                when(col("type") == "CREDIT", col("amount")).otherwise(lit(0.0))
            ).alias("customer_credit_txn_amount_30d"),
            sum(
                when(col("type") == "DEBIT", col("amount")).otherwise(lit(0.0))
            ).alias("customer_debit_txn_amount_30d"),
            sum(
                when(col("type") == "CREDIT", col("amount"))
                .when(col("type") == "DEBIT", -col("amount"))
                .otherwise(lit(0.0))
            ).alias("customer_net_cashflow_30d"),
            sum(
                when(col("k_symbol") == "SALARY", col("amount")).otherwise(lit(0.0))
            ).alias("customer_salary_inflow_30d"),
        )
    )

    loan_agg = (
        loan_df.join(customer_accounts, "account_id")
        .filter(col("status").isin("A", "C"))
        .groupBy("client_id")
        .agg(
            countDistinct("loan_id").alias("customer_loan_count_active"),
            sum("amount").alias("customer_total_loan_amount_active"),
        )
    )

    order_agg = (
        order_df.join(customer_accounts, "account_id")
        .groupBy("client_id")
        .agg(sum("amount").alias("customer_standing_order_amount_monthly"))
    )

    features = (
        client_df.withColumn("feature_date", feature_date)
        .withColumn("birth_date", _parse_birth_date_expr(col("birth_number")))
        .withColumn(
            "customer_age_years",
            floor(
                datediff(col("feature_date"), col("birth_date")) / lit(365.25)
            ),
        )
        .join(account_first, "client_id", "left")
        .withColumn("customer_tenure_days", datediff(col("feature_date"), col("first_opened_date")))
        .drop("first_opened_date")
        .join(disp_agg, "client_id", "left")
        .join(card_agg, "client_id", "left")
        .join(balance_30d, "client_id", "left")
        .join(recent_txn_agg, "client_id", "left")
        .join(loan_agg, "client_id", "left")
        .join(order_agg, "client_id", "left")
    )

    features = features.withColumn(
        "customer_num_accounts", coalesce(col("customer_num_accounts"), lit(0))
    ).withColumn(
        "customer_num_owner_accounts",
        coalesce(col("customer_num_owner_accounts"), lit(0)),
    ).withColumn(
        "customer_num_disponent_accounts",
        coalesce(col("customer_num_disponent_accounts"), lit(0)),
    ).withColumn(
        "customer_has_card_flag",
        coalesce(col("customer_has_card_flag"), lit(0)),
    ).withColumn(
        "customer_num_cards",
        coalesce(col("customer_num_cards"), lit(0)),
    ).withColumn(
        "customer_avg_account_balance_30d",
        coalesce(col("customer_avg_account_balance_30d"), lit(0.0)),
    ).withColumn(
        "customer_max_account_balance_30d",
        coalesce(col("customer_max_account_balance_30d"), lit(0.0)),
    ).withColumn(
        "customer_total_txn_count_30d",
        coalesce(col("customer_total_txn_count_30d"), lit(0)),
    ).withColumn(
        "customer_total_txn_amount_30d",
        coalesce(col("customer_total_txn_amount_30d"), lit(0.0)),
    ).withColumn(
        "customer_avg_txn_amount_30d",
        coalesce(col("customer_avg_txn_amount_30d"), lit(0.0)),
    ).withColumn(
        "customer_credit_txn_amount_30d",
        coalesce(col("customer_credit_txn_amount_30d"), lit(0.0)),
    ).withColumn(
        "customer_debit_txn_amount_30d",
        coalesce(col("customer_debit_txn_amount_30d"), lit(0.0)),
    ).withColumn(
        "customer_net_cashflow_30d",
        coalesce(col("customer_net_cashflow_30d"), lit(0.0)),
    ).withColumn(
        "customer_salary_inflow_30d",
        coalesce(col("customer_salary_inflow_30d"), lit(0.0)),
    ).withColumn(
        "customer_loan_count_active",
        coalesce(col("customer_loan_count_active"), lit(0)),
    ).withColumn(
        "customer_total_loan_amount_active",
        coalesce(col("customer_total_loan_amount_active"), lit(0.0)),
    ).withColumn(
        "customer_standing_order_amount_monthly",
        coalesce(col("customer_standing_order_amount_monthly"), lit(0.0)),
    )

    return features.select(
        "client_id",
        "feature_date",
        "customer_age_years",
        "customer_tenure_days",
        "customer_num_accounts",
        "customer_num_owner_accounts",
        "customer_num_disponent_accounts",
        "customer_has_card_flag",
        "customer_num_cards",
        "customer_card_recency_days",
        "customer_avg_account_balance_30d",
        "customer_max_account_balance_30d",
        "customer_total_txn_count_30d",
        "customer_total_txn_amount_30d",
        "customer_avg_txn_amount_30d",
        "customer_credit_txn_amount_30d",
        "customer_debit_txn_amount_30d",
        "customer_net_cashflow_30d",
        "customer_salary_inflow_30d",
        "customer_loan_count_active",
        "customer_total_loan_amount_active",
        "customer_standing_order_amount_monthly",
    )


def _build_account_metrics(
    spark: SparkSession, tables: dict[str, DataFrame] | None = None
) -> DataFrame:
    tables = tables or _load_berka_tables(spark)
    feature_date = current_date()
    account_df = tables["account"]
    trans_df = tables["trans"]
    order_df = tables["order"]

    balance_30d = (
        _filter_last_n_days(_latest_daily_balance(trans_df), "trans_date", feature_date, 30)
        .groupBy("account_id")
        .agg(
            avg("balance").alias("account_avg_balance_30d"),
        )
    )

    recent_trans = _filter_last_n_days(trans_df, "trans_date", feature_date, 30)
    txn_agg = (
        recent_trans.groupBy("account_id").agg(
            count("trans_id").alias("account_txn_count_30d"),
            sum("amount").alias("account_txn_amount_sum_30d"),
        )
    )

    order_agg = (
        order_df.groupBy("account_id")
        .agg(sum("amount").alias("account_standing_order_amount_sum"))
    )

    account_features = (
        account_df.withColumn("feature_date", feature_date)
        .join(balance_30d, "account_id", "left")
        .join(txn_agg, "account_id", "left")
        .join(order_agg, "account_id", "left")
        .withColumn("account_tenure_days", datediff(col("feature_date"), col("opened_date")))
        .withColumn("account_avg_balance_30d", coalesce(col("account_avg_balance_30d"), lit(0.0)))
        .withColumn("account_txn_count_30d", coalesce(col("account_txn_count_30d"), lit(0)))
        .withColumn("account_txn_amount_sum_30d", coalesce(col("account_txn_amount_sum_30d"), lit(0.0)))
        .withColumn(
            "account_standing_order_amount_sum",
            coalesce(col("account_standing_order_amount_sum"), lit(0.0)),
        )
    )

    return account_features.select(
        "account_id",
        "feature_date",
        "account_tenure_days",
        "account_txn_count_30d",
        "account_txn_amount_sum_30d",
        "account_avg_balance_30d",
        "account_standing_order_amount_sum",
    )


def _build_loan_metrics(
    spark: SparkSession, tables: dict[str, DataFrame] | None = None
) -> DataFrame:
    tables = tables or _load_berka_tables(spark)
    feature_date = current_date()
    loan_df = tables["loan"]

    duration_months = col("duration_months")
    loan_age = datediff(feature_date, col("start_date"))
    ratio = least(loan_age / lit(30.0), duration_months)
    ratio = when(duration_months == 0, lit(0.0)).otherwise(ratio / duration_months)

    loan_features = (
        loan_df.withColumn("feature_date", feature_date)
        .withColumn("loan_age_days", loan_age)
        .withColumn("loan_duration_months", duration_months)
        .withColumn("loan_monthly_payment_amount", col("payments"))
        .withColumn("loan_paid_installment_ratio", ratio)
        .withColumn("loan_amount", col("amount"))
    )

    return loan_features.select(
        "loan_id",
        "feature_date",
        "loan_age_days",
        "loan_amount",
        "loan_duration_months",
        "loan_monthly_payment_amount",
        "loan_paid_installment_ratio",
    )


def _persist_metrics_table(df: DataFrame, table_name: str) -> None:
    df.write.mode("overwrite").format("parquet").saveAsTable(table_name)


def create_berka_metrics_tables() -> None:
    spark = build_spark_session()
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {MANISHM_SCHEMA}")
        tables = _load_berka_tables(spark)
        customer_features = _build_berka_customer_metrics(spark, tables)
        account_features = _build_account_metrics(spark, tables)
        loan_features = _build_loan_metrics(spark, tables)

        for df, table_name in (
            (customer_features, f"{MANISHM_SCHEMA}.berka_customer_metrics"),
            (account_features, f"{MANISHM_SCHEMA}.account_metrics"),
            (loan_features, f"{MANISHM_SCHEMA}.loan_metrics"),
        ):
            _persist_metrics_table(df, table_name)
            print(f"Wrote {df.count()} rows to {table_name}")
    finally:
        spark.stop()


class SparkDFDataset:
    def __init__(self, df: DataFrame):
        self.df = df


def main() -> None:
    args = parse_arguments()
    if args.berka_metrics:
        create_berka_metrics_tables()
        return
    config = load_config(args.config)
    spark = build_spark_session()
    try:
        df = spark.sql(args.query)
        validator = SparkDFDataset(df)
        columns = set(df.columns)
        print(f"Loaded {len(df.columns)} columns, config={config}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
