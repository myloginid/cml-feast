# Feast Feature Management (Spark + HMS)
- Deploy Feast feature management for batch features using Spark in Cloudera AI (see https://feast.dev/).

<img width="1198" height="1583" alt="image" src="https://github.com/user-attachments/assets/dabf6e2e-c074-48ad-9cea-cd25ebcad02d" />


## Environment snapshot

- Python 3.11.14 with dependencies preinstalled under `/home/cdsw/.local/lib/python3.11/site-packages`.
- PySpark 3.5.1 / Scala 2.12.18 with Kerberos enabled; interacting with Hive/S3 requires a valid Kerberos ticket.
- `spark-defaults.conf` wires the CAI workspace defaults (S3, Kerberos, Hive) you need for these jobs.


## Project layout

- `create_hms_tables.py` builds the `manishm` Hive schema and loads every CSV from `berka/` into Parquet tables (account, card, client, disp, district, loan, order, trans).
- `feast_demo.py` orchestrates the Berka metric pipelines (customer/account/loan) or runs ad-hoc Spark SQL queries for validation.
- `feast_repo/` stores the Feast repo files (`feature_store.yaml`, `feature_defs.py`, offline data, registry/online sqlite DBs).
- `start_feast_ui.py` launches the Feast UX so you can browse FeatureViews/FeatureServices after updating the registry with `feast apply`.


## Setup and ingestion

1. Run `python create_hms_tables.py` so the `manishm` schema contains each Berka table (account, card, client, disp, district, loan, order, trans). Every load prints a row count for confirmation.
2. Execute `python feast_demo.py --berka-metrics` to derive customer, account, and loan metrics and persist them to the HMS tables `manishm.berka_customer_metrics`, `manishm.account_metrics`, and `manishm.loan_metrics`.

### Berka metric groups

- **Customer metrics (`manishm.berka_customer_metrics`)** – age/tenure, ownership/disponent counts, card flags, 30-day balance and txn aggregates (credit/debit splits, net cashflow, salary inflow), loan totals, and standing-order sums per client.
- **Account metrics (`manishm.account_metrics`)** – tenure in days, 30-day txn counts/sums, the latest daily balance averaged over the trailing 30 days, and standing-order totals for every account.
- **Loan metrics (`manishm.loan_metrics`)** – loan age, amount, duration, monthly payment, and the paid-installment ratio computed from the loan’s start date.


## Feast repo and registry

- Run `cd feast_repo && feast apply` (after `create_hms_tables.py` and `feast_demo.py --berka-metrics`) to register the FeatureViews/entities in `registry.db`. The Feast UI loads this registry and only shows what was last applied.
- The repo now exposes three Berka FeatureViews (`berka_customer_metrics`, `berka_account_metrics`, `berka_loan_metrics`) backed by the same HMS tables and tied to the `client`, `account_id`, and `loan_id` entities.
- Restart `python start_feast_ui.py` (or `feast ui`) after `feast apply` so the running UI refreshes and displays the updated FeatureViews.


## Feature engineering in a multi-user environment

- Treat `feast_repo/` as the canonical source. Teams can branch/PR updates to `feature_defs.py`, run `feast apply` from a shared workspace or CI job, and then refresh the shared UI to surface the new FeatureViews.
- Everyone sees the same catalog once the registry is updated; the UI caches metadata so changes only appear after restarting the server (or re-launching `feast ui`).
- Individuals can prototype safely by running their own repo copy and `feast apply`; these isolated runs do not impact the shared registry.

<img width="3235" height="1735" alt="image" src="https://github.com/user-attachments/assets/b23c554b-997f-42bd-ace3-8729aba9d22e" />


## Interactive-like experimentation

- Although Feast lacks a drag-and-drop builder, you can “interactively” define new features by writing short helpers that adjust `feature_defs.py` or append `Field` definitions, then run `feast apply`.
- After each change, restart `python start_feast_ui.py` and refresh the browser to see the expanded feature list.
