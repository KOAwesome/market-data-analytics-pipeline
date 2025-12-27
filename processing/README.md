## Market Data Analytics Pipeline (Spark + Delta Lake) ##
An end-to-end data engineering pipeline that ingests market data, processes it using Apache Spark, and stores it in a Delta Lake–based Bronze → Silver → Gold architecture.

This project demonstrates production-grade patterns such as:
- layered data modeling
- schema enforcement
- idempotent upserts (MERGE)
- window-based analytics
- Delta Lake ACID guarantees
##  Architecture Overview

Yahoo Finance >>Bronze (Delta) >>Silver (Delta) >>Gold (Delta)

### Layer Responsibilities

| Layer | Purpose |
|-----|--------|
| Bronze | Raw, structured data from source |
| Silver | Cleaned, deduplicated, analytics-ready |
| Gold | Aggregated, business-consumable metrics |

---

##  Project Structure
.
├── data_quality
├── ingestion
│   └── ingestion_market_data.py
├── notebooks
├── processing
│   ├── gold_prices.py
│   ├── results.py
│   ├── silver_prices_merge.py
│   ├── silver_prices.py
│   └── tempCodeRunnerFile.py
├── README.md
├── sql
└── transformation