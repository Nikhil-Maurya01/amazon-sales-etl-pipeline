# Amazon Sales ETL Pipeline (PySpark + PostgreSQL) with Dashboard


![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg?style=for-the-badge&logo=python&logoColor=ffdd54)
![PySpark](https://img.shields.io/badge/PySpark-ETL-orange.svg?style=for-the-badge&logo=python&logoColor=ffdd54)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-DB-blue.svg?style=for-the-badge&logo=postgresql&logoColor=ffdd54)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow.svg?style=for-the-badge&logo=powerbi&logoColor=ffdd54)
![License](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge&logo=&logoColor=ffdd54)


A production‑style **ETL pipeline** that extracts Amazon product sales data from **PostgreSQL**, cleans and standardizes it with **PySpark**, and writes a curated dataset back to PostgreSQL. The repo includes a **PowerBI dashboard** (at the end) for exploring KPIs, trends, and product performance.

---

## ✨ Highlights

- **PySpark ETL** with clear, opinionated data quality rules.
- **JDBC I/O**: PostgreSQL ➜ Spark DataFrame ➜ PostgreSQL (cleaned table).
- **Curated sample CSV** for quick analytics & the dashboard.
- **PowerBI dashboard** to visualize insights (KPIs, filters, charts).

---

## ⚙️ Tech Stack
- **Python 3.9+**
- **PySpark** for distributed data processing
- **PostgreSQL** for relational storage
- **Power BI** for visualization
- **JDBC Driver** for Spark-Postgres connectivity

---

## 📊 Amazon Sales Dashboard

### Overview
This dashboard provides a comprehensive view of Amazon's sales performance after the ETL process. It highlights:

- **Total Discounts**: $61M  
- **Total Sales Amount**: $612M  
- **Total Products Sold**: 10M  

The visualizations include:
- Category Ratings
- Maximum Discount per Category
- Total Sales per Product
- Best Seller Count per Category
- Detailed Category Ratings

![Dashboard_picture](Dashboard_picture.png)
