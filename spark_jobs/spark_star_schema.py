from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("LoadStarSchema").getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/lab2"
pg_props = {"user": "labuser", "password": "labpass", "driver": "org.postgresql.Driver"}

staging_df = spark.read.jdbc(url=pg_url, table="staging.mock_data", properties=pg_props)

# PRODUCTS
products_dim = staging_df.select(
    F.trim("product_name").alias("product_name"),
    F.trim("product_category").alias("category"),
    F.col("product_rating").alias("avg_rating"),
    F.col("product_reviews").alias("review_count"),
    F.trim("supplier_name").alias("supplier_name")
).distinct() \
  .withColumn("product_id",
              F.row_number().over(Window.orderBy("product_name", "supplier_name")).cast("int"))

# SUPPLIERS
suppliers_dim = staging_df.select(
    F.trim("supplier_name").alias("supplier_name"),
    F.trim("supplier_country").alias("country")
).distinct() \
  .withColumn("supplier_id",
              F.row_number().over(Window.orderBy("supplier_name")).cast("int"))

# STORES
stores_dim = staging_df.select(
    F.trim("store_name").alias("store_name"),
    F.trim("store_city").alias("city"),
    F.trim("store_country").alias("country")
).distinct() \
  .withColumn("store_id",
              F.row_number().over(Window.orderBy("store_name", "city")).cast("int"))

# CUSTOMERS
customers_dim = staging_df.select(
    F.trim("customer_first_name").alias("first_name"),
    F.trim("customer_last_name").alias("last_name"),
    F.trim("customer_country").alias("country")
).distinct() \
  .withColumn("customer_id",
              F.row_number().over(Window.orderBy("first_name", "last_name", "country")).cast("int")) \
  .withColumn("customer_name", F.concat_ws(" ", "first_name", "last_name"))

# SELLERS
sellers_dim = staging_df.select(
    F.trim("seller_first_name").alias("first_name"),
    F.trim("seller_last_name").alias("last_name")
).distinct() \
  .withColumn("seller_id",
              F.row_number().over(Window.orderBy("first_name", "last_name")).cast("int")) \
  .withColumn("seller_name", F.concat_ws(" ", "first_name", "last_name"))

# TIME
time_dim = staging_df.select(F.col("sale_date").alias("date")).distinct() \
         .withColumn("year",  F.year("date")) \
         .withColumn("month", F.month("date")) \
         .withColumn("time_id",
                     F.row_number().over(Window.orderBy("date")).cast("int"))

fact_df = staging_df \
    .join(products_dim,  (F.trim(staging_df.product_name)  == products_dim.product_name)  &
                         (F.trim(staging_df.supplier_name) == products_dim.supplier_name), "left") \
    .join(suppliers_dim, F.trim(staging_df.supplier_name) == suppliers_dim.supplier_name, "left") \
    .join(stores_dim,    (F.trim(staging_df.store_name)   == stores_dim.store_name)     &
                         (F.trim(staging_df.store_city)   == stores_dim.city), "left")  \
    .join(customers_dim, (F.trim(staging_df.customer_first_name) == customers_dim.first_name) &
                         (F.trim(staging_df.customer_last_name)  == customers_dim.last_name)  &
                         (F.trim(staging_df.customer_country)    == customers_dim.country), "left") \
    .join(sellers_dim,   (F.trim(staging_df.seller_first_name)   == sellers_dim.first_name)  &
                         (F.trim(staging_df.seller_last_name)    == sellers_dim.last_name), "left") \
    .join(time_dim,      staging_df.sale_date == time_dim.date, "left") \
    .select(
        F.col("id").alias("row_id"),
        suppliers_dim.supplier_id,
        stores_dim.store_id,
        products_dim.product_id,
        customers_dim.customer_id,
        sellers_dim.seller_id,
        time_dim.time_id,
        F.col("product_price").cast("double").alias("price"),
        F.col("sale_quantity").cast("int").alias("quantity"),
        (F.col("product_price") * F.col("sale_quantity")).alias("total_price")
    )

for df, name in [
    (customers_dim, "dim_customers"),
    (sellers_dim,   "dim_sellers"),
    (products_dim,  "dim_products"),
    (stores_dim,    "dim_stores"),
    (suppliers_dim, "dim_suppliers"),
    (time_dim,      "dim_time"),
]:
    df.write.jdbc(pg_url, f"data_warehouse.{name}", mode="overwrite", properties=pg_props)

fact_df.write.jdbc(pg_url, "data_warehouse.fact_sales", mode="overwrite", properties=pg_props)

print("✅ Star schema successfully loaded into PostgreSQL (schema data_warehouse).")
spark.stop()
