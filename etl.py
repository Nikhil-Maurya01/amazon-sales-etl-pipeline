
"""
Amazon Product Sales ETL Pipeline
---------------------------------
This script:
- Reads raw data from PostgreSQL
- Cleans and transforms data using PySpark
- Writes cleaned data back to PostgreSQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, to_date, lit, lower, avg
)
from pyspark.sql.types import DoubleType

# Initialize Spark session with correct JDBC driver path
spark = SparkSession.builder \
    .appName("AmazonProductDataETL") \
    .config("spark.jars", "jars/postgresql-42.7.7.jar") \
    .getOrCreate()

# PostgreSQL connection properties
url = "jdbc:postgresql://localhost:5432/Sales"
properties = {
    "user": "postgres",
    "password": "****",
    "driver": "org.postgresql.Driver"
}

# Load data from PostgreSQL table
df = spark.read.jdbc(url=url, table="amazon_products_sales", properties=properties)

# Drop rows with missing product_title only
df = df.dropna(subset=["product_title"])

# Drop the sustainability_tags column if it exists
if "sustainability_tags" in df.columns:
    df = df.drop("sustainability_tags")

# Filter out non-numeric product_rating values before casting
df = df.filter(col("product_rating").rlike("^[0-9]+(\\.[0-9]+)?$"))
df = df.withColumn("product_rating", col("product_rating").cast(DoubleType()))

# Compute average rating
avg_rating = df.select(avg("product_rating")).first()[0]

# Fill missing ratings with average
df = df.withColumn("product_rating", when(col("product_rating").isNull(), lit(avg_rating)).otherwise(col("product_rating")))

# Fill missing numerical values
df = df.fillna({
    "total_reviews": 0,
    "purchased_last_month": 0,
    "discount_percentage": 0
})

# Impute missing prices
df = df.withColumn("discounted_price", when(col("discounted_price").isNull(), col("original_price")).otherwise(col("discounted_price")))
df = df.withColumn("original_price", when(col("original_price").isNull(), col("discounted_price")).otherwise(col("original_price")))

# Standardize boolean columns
df = df.withColumn("buy_box_availability", when(col("buy_box_availability") == "Add to cart", lit(True)).otherwise(lit(False)))
df = df.withColumn("is_best_seller", when(col("is_best_seller") == "Best Seller", lit(True)).otherwise(lit(False)))
df = df.withColumn("is_sponsored", when(col("is_sponsored") == "Sponsored", lit(True)).otherwise(lit(False)))
df = df.withColumn("has_coupon", when(col("has_coupon").isNull() | (col("has_coupon") == "No Coupon"), lit(False)).otherwise(lit(True)))

# Convert date columns
df = df.withColumn("delivery_date", to_date(col("delivery_date"), "yyyy-MM-dd"))
df = df.withColumn("data_collected_at", to_date(col("data_collected_at")))

# Fill missing data_collected_at with mode date
mode_date = df.groupBy("data_collected_at").count().orderBy("count", ascending=False).first()[0]
df = df.withColumn("data_collected_at", when(col("data_collected_at").isNull(), lit(mode_date)).otherwise(col("data_collected_at")))

# Standardize category names
df = df.withColumn("product_category", trim(regexp_replace(col("product_category"), "_", " ")))
df = df.withColumn("product_category", lower(col("product_category")))

# Remove duplicates
df = df.dropDuplicates(["product_title", "product_category"])

# Write the cleaned data back to PostgreSQL under a new table name
df.write.jdbc(url=url, table="amazon_product_sales_cleaned", mode="overwrite", properties=properties)
