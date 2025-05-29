from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("CreateReports").getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/lab2"
pg_props = {"user": "labuser", "password": "labpass", "driver": "org.postgresql.Driver"}

ch_url = "jdbc:clickhouse://clickhouse:8123/lab2_reports"
ch_props = {"user": "default", "password": "", "driver": "ru.yandex.clickhouse.ClickHouseDriver"}

fact_df = spark.read.jdbc(url=pg_url, table="data_warehouse.fact_sales", properties=pg_props)
prod_dim = spark.read.jdbc(url=pg_url, table="data_warehouse.dim_products", properties=pg_props)
cust_dim = spark.read.jdbc(url=pg_url, table="data_warehouse.dim_customers", properties=pg_props)
store_dim = spark.read.jdbc(url=pg_url, table="data_warehouse.dim_stores", properties=pg_props)
sup_dim = spark.read.jdbc(url=pg_url, table="data_warehouse.dim_suppliers", properties=pg_props)
time_dim = spark.read.jdbc(url=pg_url, table="data_warehouse.dim_time", properties=pg_props)

# 1. Витрина по продуктам (sales_by_product)
# Агрегируем факт по продукту: суммарная выручка и количество продаж
prod_metrics = fact_df.groupBy("product_id") \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("*").alias("sales_count")
    )
# Соединяем с размерностью продуктов, чтобы получить название, категорию, рейтинг, отзывы
sales_by_product_df = prod_metrics.join(prod_dim, "product_id", "left") \
    .select(
        "product_id",
        "product_name",
        "category",
        "total_revenue",
        "sales_count",
        "avg_rating",
        "review_count"
    )
# Записываем в ClickHouse (таблица lab2_reports.sales_by_product)
sales_by_product_df.write.jdbc(url=ch_url, table="lab2_reports.sales_by_product", 
                               mode="append", properties=ch_props)

# 2. Витрина по клиентам (sales_by_customer)
# Агрегируем факт по клиенту: суммарная выручка и число заказов (distinct row_id)
cust_metrics = fact_df.groupBy("customer_id") \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.countDistinct("row_id").alias("order_count")
    )
# Вычисляем средний чек (avg_check) = total_revenue / order_count
cust_metrics = cust_metrics.withColumn("avg_check", 
                                       F.when(F.col("order_count") > 0, 
                                              F.col("total_revenue")/F.col("order_count")) \
                                        .otherwise(F.lit(0.0))
                                      )
# Присоединяем имя и страну клиента из размерности
sales_by_cust_df = cust_metrics.join(cust_dim, "customer_id", "left") \
    .select(
        "customer_id",
        "customer_name",
        F.col("country").alias("country"),
        "total_revenue",
        "order_count",
        F.col("avg_check").alias("avg_check")
    )
sales_by_cust_df.write.jdbc(url=ch_url, table="lab2_reports.sales_by_customer",
                            mode="append", properties=ch_props)

# 3. Витрина по времени (sales_by_time)
# Агрегируем факт по месяцу (используем таблицу времени для группировки по year, month)
# Соединяем факт с измерением времени, чтобы получить year и month для каждого факта
fact_with_time = fact_df.join(time_dim, fact_df.time_id == time_dim.time_id, "left")
time_metrics = fact_with_time.groupBy("year", "month") \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.countDistinct("row_id").alias("order_count"),
        F.count("*").alias("item_count")
    )
# Вычисляем среднее количество товаров на заказ за месяц
time_metrics = time_metrics.withColumn("avg_items_per_order",
                                       F.when(F.col("order_count") > 0,
                                              F.col("item_count")/F.col("order_count")) \
                                        .otherwise(F.lit(0.0))
                                      )
sales_by_time_df = time_metrics.select(
    "year", "month", "total_revenue", "order_count", "item_count", "avg_items_per_order"
)
sales_by_time_df.write.jdbc(url=ch_url, table="lab2_reports.sales_by_time",
                            mode="append", properties=ch_props)

# 4. Витрина по магазинам (sales_by_store)
store_metrics = fact_df.groupBy("store_id") \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.countDistinct("row_id").alias("order_count")
    )
# Средний чек по магазину
store_metrics = store_metrics.withColumn("avg_check",
                                         F.when(F.col("order_count") > 0,
                                                F.col("total_revenue")/F.col("order_count")) \
                                          .otherwise(F.lit(0.0))
                                        )
# Присоединяем информацию о магазине (название, город, страна)
sales_by_store_df = store_metrics.join(store_dim, "store_id", "left") \
    .select(
        "store_id",
        "store_name",
        "city",
        "country",
        "total_revenue",
        "order_count",
        "avg_check"
    )
sales_by_store_df.write.jdbc(url=ch_url, table="lab2_reports.sales_by_store",
                             mode="append", properties=ch_props)

# 5. Витрина по поставщикам (sales_by_supplier)
sup_metrics = fact_df.groupBy("supplier_id") \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("*").alias("sales_count")
    )
# Средняя цена товара у данного поставщика = общая выручка / количество проданных единиц
sup_metrics = sup_metrics.withColumn("avg_price", 
                                     F.when(F.col("sales_count") > 0,
                                            F.col("total_revenue")/F.col("sales_count")) \
                                      .otherwise(F.lit(0.0))
                                    )
# Добавляем информацию о поставщике (название, страна)
sales_by_sup_df = sup_metrics.join(sup_dim, "supplier_id", "left") \
    .select(
        "supplier_id",
        "supplier_name",
        F.col("country").alias("country"),
        "total_revenue",
        "sales_count",
        "avg_price"
    )
sales_by_sup_df.write.jdbc(url=ch_url, table="lab2_reports.sales_by_supplier",
                           mode="append", properties=ch_props)

# 6. Витрина качества продукции (product_quality)
# Для анализа качества возьмем данные из витрины по продуктам: рейтинг, отзывы, продажи
product_quality_df = sales_by_product_df.select(
    "product_id", "product_name", "category", "avg_rating", "review_count", "sales_count"
)

product_quality_df.write.jdbc(url=ch_url, table="lab2_reports.product_quality",
                              mode="append", properties=ch_props)

print("Sales report tables have been created in ClickHouse (lab2_reports database).")
spark.stop()
