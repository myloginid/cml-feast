"""Example Feast feature definitions inspired by the shared Spark FeatureView snippet."""

from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, ValueType
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.types import Float32, Int64


# Entities -----------------------------------------------------------
berka_client = Entity(
    name="client",
    value_type=ValueType.INT64,
    description="Berka banking client identifier.",
)

account = Entity(
    name="account_id",
    value_type=ValueType.INT64,
    description="Berka account identifier.",
)

loan = Entity(
    name="loan_id",
    value_type=ValueType.INT64,
    description="Berka loan identifier.",
)


# Sources ------------------------------------------------------------
berka_customer_metrics_source = SparkSource(
    name="berka_customer_metrics",
    table="manishm.berka_customer_metrics",
    timestamp_field="feature_date",
)

account_metrics_source = SparkSource(
    name="berka_account_metrics",
    table="manishm.account_metrics",
    timestamp_field="feature_date",
)

loan_metrics_source = SparkSource(
    name="berka_loan_metrics",
    table="manishm.loan_metrics",
    timestamp_field="feature_date",
)


# FeatureViews -------------------------------------------------------
berka_customer_metrics_view = FeatureView(
    name="berka_customer_metrics",
    entities=[berka_client],
    ttl=timedelta(days=30),
    schema=[
        Field(name="customer_age_years", dtype=Int64),
        Field(name="customer_tenure_days", dtype=Int64),
        Field(name="customer_num_accounts", dtype=Int64),
        Field(name="customer_num_owner_accounts", dtype=Int64),
        Field(name="customer_num_disponent_accounts", dtype=Int64),
        Field(name="customer_has_card_flag", dtype=Int64),
        Field(name="customer_num_cards", dtype=Int64),
        Field(name="customer_card_recency_days", dtype=Int64),
        Field(name="customer_avg_account_balance_30d", dtype=Float32),
        Field(name="customer_max_account_balance_30d", dtype=Float32),
        Field(name="customer_total_txn_count_30d", dtype=Int64),
        Field(name="customer_total_txn_amount_30d", dtype=Float32),
        Field(name="customer_avg_txn_amount_30d", dtype=Float32),
        Field(name="customer_credit_txn_amount_30d", dtype=Float32),
        Field(name="customer_debit_txn_amount_30d", dtype=Float32),
        Field(name="customer_net_cashflow_30d", dtype=Float32),
        Field(name="customer_salary_inflow_30d", dtype=Float32),
        Field(name="customer_loan_count_active", dtype=Int64),
        Field(name="customer_total_loan_amount_active", dtype=Float32),
        Field(name="customer_standing_order_amount_monthly", dtype=Float32),
    ],
    online=True,
    source=berka_customer_metrics_source,
    tags={"origin": "berka"},
)

account_metrics_view = FeatureView(
    name="berka_account_metrics",
    entities=[account],
    ttl=timedelta(days=30),
    schema=[
        Field(name="account_tenure_days", dtype=Int64),
        Field(name="account_txn_count_30d", dtype=Int64),
        Field(name="account_txn_amount_sum_30d", dtype=Float32),
        Field(name="account_avg_balance_30d", dtype=Float32),
        Field(name="account_standing_order_amount_sum", dtype=Float32),
    ],
    online=True,
    source=account_metrics_source,
    tags={"origin": "berka"},
)

loan_metrics_view = FeatureView(
    name="berka_loan_metrics",
    entities=[loan],
    ttl=timedelta(days=30),
    schema=[
        Field(name="loan_age_days", dtype=Int64),
        Field(name="loan_amount", dtype=Float32),
        Field(name="loan_duration_months", dtype=Float32),
        Field(name="loan_monthly_payment_amount", dtype=Float32),
        Field(name="loan_paid_installment_ratio", dtype=Float32),
    ],
    online=True,
    source=loan_metrics_source,
    tags={"origin": "berka"},
)


# Feature service ----------------------------------------------------
berka_feature_service = FeatureService(
    name="berka_service",
    features=[berka_customer_metrics_view, account_metrics_view, loan_metrics_view],
)
