# insite2profit
Take-home assignment for INSITE2PROFIT

## Environment
Development and data storage use [Databricks Community Edition](https://community.cloud.databricks.com/) (free tier) with the provided Serverless cluster.

## Repository Structure

- [data/raw/](data/raw/) — unchanged raw data files as provided in the assignment email. No other datasets are included in this repo; all additional data is created and stored in Databricks Community Edition.
- [docs/assignment.md](docs/assignment.md) — assignment instructions as provided in the email.
- [notebooks/](notebooks/) — Databricks notebooks used for this solution.

## Notebooks Overview

| Notebook | Purpose |
|---|---|
| [01_examine_raw_data.py](notebooks/01_examine_raw_data.py) | Explores raw tables, identifies PKs/FKs, and investigates duplicate `ProductID` values |
| [02_create_store_tables.py](notebooks/02_create_store_tables.py) | Creates silver-layer `store_` tables with deduplication, PKs, and FKs enforced |
| [03_create_publish_tables.py](notebooks/03_create_publish_tables.py) | Creates gold-layer `publish_product` and `publish_orders` with business logic transformations |
| [04_analysis.py](notebooks/04_analysis.py) | Answers the two analysis questions against the publish tables |

## Data Architecture

Follows the medallion architecture pattern:
- **Bronze** (`raw_`): raw source data, preserved as-is for audit
- **Silver** (`store_`): cleaned, deduplicated, PKs and FKs enforced
- **Gold** (`publish_`): business logic applied, ready for analysis

The three raw CSV files were uploaded to Databricks and registered as tables with the `raw_` prefix (`raw_products`, `raw_sales_order_detail`, `raw_sales_order_header`). Databricks-inferred schema types were reviewed and accepted as appropriate — numerics for quantities, prices, and IDs; strings for descriptive and categorical fields; dates for order and ship dates.

## Key Findings

### Raw Data Notes
- `raw_products` contains duplicate `ProductID` rows where the same product appears once with a `ProductCategoryName` and once without. The `store_products` silver table resolves this by keeping the row with the non-null category name.

### Question 1 — Highest Revenue Color by Year

| Year | Color  | Total Revenue  |
|------|--------|----------------|
| 2021 | Red    | $ 6,019,614.02 |
| 2022 | Black  | $14,005,242.98 |
| 2023 | Black  | $15,047,694.37 |
| 2024 | Yellow | $ 6,368,158.48 |

Revenue is calculated as `OrderQty * (UnitPrice - UnitPriceDiscount)` per the assignment specification.

### Question 2 — Average Lead Time in Business Days by Product Category

| ProductCategoryName | Avg Lead Time (Business Days) |
|---------------------|-------------------------------|
| null                | 5.010809231668127             |
| Accessories         | 5.0065279164426695            |
| Bikes               | 5.004896845147306             |
| Clothing            | 5.005067001675042             |
| Components          | 5.0032938844517               |

Lead time is counted as weekdays from `OrderDate` (inclusive) to `ShipDate` (exclusive).

**Note on null ProductCategoryName:** The assignment provided three subcategory-to-category mapping rules (Clothing, Accessories, Components). Some products have subcategory values that do not appear in any of those three lists (e.g. "Jerseys"), so their `ProductCategoryName` remains null after transformation. These were not invented or forced into a category — the null rows reflect products outside the scope of the provided mapping rules.

