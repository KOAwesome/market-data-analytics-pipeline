# Market Data Analytics Pipeline (Spark + Delta Lake)

## Overview

This project implements an end-to-end **market data analytics pipeline** using **Apache Spark and Delta Lake**, following a **Bronze–Silver–Gold architecture**.

The pipeline ingests historical equity market data, applies correctness-focused transformations and data quality validation, and produces analytics-ready datasets.

The primary goal of this project is to demonstrate **data engineering fundamentals**, not notebook-driven analysis or ad-hoc SQL exploration.

---

## Architecture

External Source (Yahoo Finance)  
→ Bronze (Delta, structured raw data)  
→ Silver (cleaned, deduplicated, analytics-ready)  
→ Gold (aggregated business metrics)

---

## Technology Stack

- Python
- Apache Spark (PySpark)
- Delta Lake
- Spark SQL (via DataFrame API)
- Window functions for time-series analytics

