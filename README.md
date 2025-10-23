# Amazon Sales ETL Pipeline (PySpark + PostgreSQL) with Dashboard


![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg?style=for-the-badge&logo=python&logoColor=ffdd54)
![PySpark](https://img.shields.io/badge/PySpark-ETL-orange.svg?style=for-the-badge&logo=python&logoColor=ffdd54)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-DB-blue.svg?style=for-the-badge&logo=postgresql&logoColor=ffdd54)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow.svg?style=for-the-badge&logo=powerbi&logoColor=ffdd54)
![License](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge&logo=&logoColor=ffdd54)


A production‑style **ETL pipeline** that extracts Amazon product sales data from **PostgreSQL**, cleans and standardizes it with **PySpark**, and writes a curated dataset back to PostgreSQL. The repo includes a **Streamlit dashboard** (at the end) for exploring KPIs, trends, and product performance.

---

## ✨ Highlights

- **PySpark ETL** with clear, opinionated data quality rules.
- **JDBC I/O**: PostgreSQL ➜ Spark DataFrame ➜ PostgreSQL (cleaned table).
- **Curated sample CSV** for quick analytics & the dashboard.
- **PowerBI dashboard** to visualize insights (KPIs, filters, charts).

---

## 🧱 Architecture
