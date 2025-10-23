# amazon-sales-etl-pipeline
# Amazon Sales ETL Pipeline (PySpark + PostgreSQL) with Dashboard

![Python](https://img.shields.io/badge/Python-3.svg
![PySpark](https://img.shields.io/bk-ETL-orange.svg
![PostgreSQL](https://img.shields.io/badge-blue.svg
![Licensemg.shields.io/badge/License-MIT-green.svg

A production‑style **ETL pipeline** that extracts Amazon product sales data from **PostgreSQL**, cleans and standardizes it with **PySpark**, and writes a curated dataset back to PostgreSQL. The repo includes a **Streamlit dashboard** (at the end) for exploring KPIs, trends, and product performance.

---

## ✨ Highlights

- **PySpark ETL** with clear, opinionated data quality rules.
- **JDBC I/O**: PostgreSQL ➜ Spark DataFrame ➜ PostgreSQL (cleaned table).
- **Curated sample CSV** for quick analytics & the dashboard.
- **PowerBI dashboard** to visualize insights (KPIs, filters, charts).

---

## 🧱 Architecture
