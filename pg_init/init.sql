-- создаём схему staging и таблицу приёмника
DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;

CREATE TABLE staging.mock_data (
    id                     BIGINT,
    customer_first_name    TEXT,
    customer_last_name     TEXT,
    customer_age           INT,
    customer_email         TEXT,
    customer_country       TEXT,
    customer_postal_code   TEXT,
    customer_pet_type      TEXT,
    customer_pet_name      TEXT,
    customer_pet_breed     TEXT,
    seller_first_name      TEXT,
    seller_last_name       TEXT,
    seller_email           TEXT,
    seller_country         TEXT,
    seller_postal_code     TEXT,
    product_name           TEXT,
    product_category       TEXT,
    product_price          NUMERIC(10,2),
    product_quantity       INT,
    sale_date              DATE,
    sale_customer_id       INT,
    sale_seller_id         INT,
    sale_product_id        INT,
    sale_quantity          INT,
    sale_total_price       NUMERIC(12,2),
    store_name             TEXT,
    store_location         TEXT,
    store_city             TEXT,
    store_state            TEXT,
    store_country          TEXT,
    store_phone            TEXT,
    store_email            TEXT,
    pet_category           TEXT,
    product_weight         NUMERIC(10,2),
    product_color          TEXT,
    product_size           TEXT,
    product_brand          TEXT,
    product_material       TEXT,
    product_description    TEXT,
    product_rating         NUMERIC(3,2),
    product_reviews        INT,
    product_release_date   DATE,
    product_expiry_date    DATE,
    supplier_name          TEXT,
    supplier_contact       TEXT,
    supplier_email         TEXT,
    supplier_phone         TEXT,
    supplier_address       TEXT,
    supplier_city          TEXT,
    supplier_country       TEXT
);

\echo '===> Загрузка CSV в staging.mock_data'
\set path '/docker-entrypoint-initdb.d'

COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data1.csv'  WITH (FORMAT csv, HEADER true);
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data2.csv'  WITH (FORMAT csv, HEADER true);
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data3.csv'  WITH (FORMAT csv, HEADER true);
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data4.csv'  WITH (FORMAT csv, HEADER true);
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data5.csv'  WITH (FORMAT csv, HEADER true);
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data6.csv'  WITH (FORMAT csv, HEADER true);
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data7.csv'  WITH (FORMAT csv, HEADER true);
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data8.csv'  WITH (FORMAT csv, HEADER true);
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data9.csv'  WITH (FORMAT csv, HEADER true);
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/mock_data10.csv' WITH (FORMAT csv, HEADER true);

SELECT 'Rows loaded = ' || (SELECT COUNT(*) FROM staging.mock_data) AS info;

CREATE SCHEMA IF NOT EXISTS data_warehouse;