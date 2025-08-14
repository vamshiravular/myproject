from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, round, countDistinct, to_date

# Create Spark Session
spark = SparkSession.builder \
    .appName("Retail Sales Transformation") \
    .getOrCreate()

# Load CSV into DataFrame
df = spark.read.csv("data/sales_data.csv", header=True, inferSchema=True)

print("===== RAW DATA =====")
df.show()

# 1️⃣ BASIC TRANSFORMATIONS
# Select specific columns
df_selected = df.select("order_id", "product", "quantity", "price")
print("===== SELECTED COLUMNS =====")
df_selected.show()

# Add new calculated column (total value per order)
df_with_total = df.withColumn("total_value", col("quantity") * col("price"))
print("===== WITH TOTAL VALUE =====")
df_with_total.show()

# Filter: only Electronics category
electronics_df = df_with_total.filter(col("category") == "Electronics")
print("===== ELECTRONICS ONLY =====")
electronics_df.show()

# 2️⃣ INTERMEDIATE TRANSFORMATIONS
# Group by category and calculate total sales
category_sales = df_with_total.groupBy("category") \
    .agg(_sum("total_value").alias("total_sales"))
print("===== TOTAL SALES PER CATEGORY =====")
category_sales.show()

# Average price per product
avg_price = df.groupBy("product") \
    .agg(round(avg("price"), 2).alias("avg_price"))
print("===== AVERAGE PRICE PER PRODUCT =====")
avg_price.show()

# 3️⃣ MEDIUM LEVEL TRANSFORMATIONS
# Daily sales summary with distinct customers
daily_sales = df_with_total.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .groupBy("order_date") \
    .agg(
        _sum("total_value").alias("daily_total_sales"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .orderBy("order_date")
print("===== DAILY SALES SUMMARY =====")
daily_sales.show()

# Stop Spark session
spark.stop()
