CREATE DATABASE IF NOT EXISTS lab2_reports;

CREATE TABLE lab2_reports.sales_by_product (
    product_id    UInt32,
    product_name  String,
    category      String,
    total_revenue Float64,
    sales_count   UInt32,
    avg_rating    Float32,
    review_count  UInt32
) ENGINE = MergeTree() 
ORDER BY product_id;

CREATE TABLE lab2_reports.sales_by_customer (
    customer_id    UInt32,
    customer_name  String,
    country        String,
    total_revenue  Float64,
    order_count    UInt32,
    avg_check      Float64
) ENGINE = MergeTree()
ORDER BY customer_id;

CREATE TABLE lab2_reports.sales_by_time (
    year             UInt16,
    month            UInt8,
    total_revenue    Float64,
    order_count      UInt32,
    item_count       UInt32,
    avg_items_per_order Float32
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE lab2_reports.sales_by_store (
    store_id      UInt32,
    store_name    String,
    city          String,
    country       String,
    total_revenue Float64,
    order_count   UInt32,
    avg_check     Float64
) ENGINE = MergeTree()
ORDER BY store_id;

CREATE TABLE lab2_reports.sales_by_supplier (
    supplier_id   UInt32,
    supplier_name String,
    country       String,
    total_revenue Float64,
    sales_count   UInt32,
    avg_price     Float64
) ENGINE = MergeTree()
ORDER BY supplier_id;

CREATE TABLE lab2_reports.product_quality (
    product_id    UInt32,
    product_name  String,
    category      String,
    avg_rating    Float32,
    review_count  UInt32,
    sales_count   UInt32
) ENGINE = MergeTree()
ORDER BY product_id;
