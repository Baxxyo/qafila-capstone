# Qafila Lakehouse — Capstone Project

**Student:**  Badryah Adel Aljabri-بدرية عادل الجابري

**Program:** Modern Data Engineering — SDAIA Academy


Option 1 (Qafila Lakehouse), built on the course starter kit
(https://github.com/SDAIAAcademy/data-engineering).

## Repository contents
| Path | What it is |
|---|---|
| `capstone/pipeline.py` | **My pipeline**: one Dagster code location (ingestion, contract, dbt, Iceberg, governance, partitions) |
| `capstone/contracts/payments.odcs.yaml` | ODCS data contract for the payments feed |
| `dbt_qafila/` | dbt project: staging, intermediate, marts, tests (from the course) |
| `prepare_data.py` | Generates the synthetic Qafila data into `data/raw/` (from the course) |
| `de_utils.py` | Shared helpers: paths, DuckDB connection, Iceberg catalog (from the course) |
| `pyproject.toml` | Pinned dependencies |

No data is committed: `prepare_data.py` regenerates it deterministically, so no personal data is ever in git.

## Problem
Qafila's ops and finance teams need trusted daily revenue per city and store
and daily delivery KPIs, refreshed nightly, without querying the production app database.

## How to run

### Google Colab (how I ran it)
    !git clone https://github.com/[اسم حسابك]/qafila-capstone
    %cd qafila-capstone
    !pip install -q uv
    !uv sync
    !uv run python prepare_data.py
    !uv run python capstone/pipeline.py            # end to end, one command
    !uv run python capstone/pipeline.py backfill   # daily ops partitions

### Locally
    uv sync
    uv run python prepare_data.py
    export QAFILA_PII_KEY=<secret>
    uv run python capstone/pipeline.py
    uv run python capstone/pipeline.py backfill
    uv run dagster dev -f capstone/pipeline.py     # Dagster UI

Expected result: `run succeeded: 32 assets materialized, 47 checks`.

## Sources (4 types)
| Source | Type | Load strategy |
|---|---|---|
| App DB: orders, order_items | database | incremental on `updated_at`, 3-day look-back, delete+insert on key; watermark committed in the same transaction |
| App DB: customers, stores, products | database | full replace (small tables) |
| Payments | API (JSON lines) | incremental on `updated_at`, merge on `payment_id` |
| Delivery events | events | raw landed as-is, deduplicated by `event_id` downstream |
| Supplier prices, reviews | files | full replace |

## Architecture
raw (DuckDB) → dbt staging → dbt marts → gold on Apache Iceberg.
Orchestrated by Dagster: one code location, nightly schedule at 01:30 Riyadh time,
and a daily-partitioned ops job.

## Data model (grain)
- `fct_order_items`: one row per order line.
- `fct_orders`: one row per order (the delivery fee lives here, so there is no fan-out).
- `mart_daily_city_sales`: one row per day per city, published to `gold.daily_city_sales` (Iceberg).
- `ops.delivery_kpis_daily`: one row per Riyadh day per store (daily partitions).

## Evidence
- **Incremental:** the first run loaded 45,766 orders and 41,785 payments; the re-run reloaded only
  the 3-day look-back (2,351 orders, 2,151 payments).
- **Idempotent:** row counts are identical after re-running
  (raw.orders 45,766 · raw.order_items 172,545 · raw.payments 41,785 · fct_order_items 172,403).
- **Reconciliation:** raw.order_items (172,545) − fct_order_items (172,403) = 142,
  exactly the quarantined lines: every row is either in the fact table or quarantined with a reason.
- **Backfill:** 7 daily partitions (Sep 24–30) → 84 rows (7 days × 12 stores);
  re-running the backfill gives the same 84 rows (delete-then-insert per day).

## Quality & observability
- **47 checks** on every build: 43 dbt data tests, 3 dbt unit tests, the payments contract,
  a volume monitor and a PII scan.
- Payments ODCS contract:
  - structure rules are **blocking**: dbt does not run on a broken feed;
  - value rules **warn** and route bad rows to `quarantine.payments`.
- **Real issues caught** in the source data:
  - 16 payments with amount ≤ 0 (routed to `quarantine.payments`);
  - 142 order lines with non-positive quantity (quarantined in `quarantine.qrn_order_items`, excluded from facts);
  - 4 supplier costs ≤ 0;
  - 258 orders whose totals don't match their payments (warn: routed to the payments owner).
- Volume monitor: each day compared with the same weekday's 4-week median, so weekends don't raise false alarms.

## Governance (PDPL / NDMO)
- Column classification by NDMO level: `governed.pii_classification`.
- National ID stays in raw; downstream uses an HMAC-SHA256 `person_token` (key from an environment variable, not code).
- IBAN stays in raw only; it is never copied to governed tables or marts.
- Analysts read `governed.customers`: masked mobile, email domain only, no names.
- Check `no_national_ids_in_marts` scans every marts column with a Luhn validator: **0 leaks**.
- Right to erasure: delete in the source, re-run (customers are fully replaced), expire Iceberg
  snapshots and remove orphan files, then verify with the scan check.
- DuckDB has no role grants; in production this becomes column masks and row policies in the platform or catalog.

## Limitations & next steps
- No streaming path yet: the next step is the micro-batch Iceberg sink from Lab 3.4 for a live delivery-SLA view.
- The partition range is fixed to the sample week; in production, drop `end_date`.
- At 100× volume: Iceberg for all raw tables, partition by `order_date`, and Spark only if a single job reads terabytes.
