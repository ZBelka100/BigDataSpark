## 1. Запустим контейнеры
```bash
docker compose up -d
```
Помимо всех необходимых контейнеров, у нас запустить sql-скрипт, который считает данные и загрузит, для дальнейшей работы
## 2. Запустим скрипт на Spark, который формирует звезду.

```bash
docker exec -it spark-master spark-submit \
    --master spark://spark-master:7077 \
    --jars /jars/postgresql-42.2.23.jar,/jars/clickhouse-jdbc-0.2.6.jar \
    /scripts/spark_star_schema.py
```

## 3. Запустим скрипт на Spark, который создаёт отчеты в ClickHouse.

```bash
docker exec -it spark-master spark-submit \
    --master spark://spark-master:7077 \
    --jars /jars/postgresql-42.2.23.jar,/jars/guava-32.1.3-jre.jar,/jars/clickhouse-jdbc-0.2.6.jar \
    /scripts/spark_clickhouse_report.py
```

После запуска скрипта, нужно убедиться в том, что все окей и данные на месте. Запустите данную команду, чтобы убедиться:
```bash
docker exec -it clickhouse clickhouse-client
SHOW DATABASES;
USE lab2_reports;
SHOW TABLES;
SELECT * FROM sales_by_time LIMIT 10
```
Последний запрос как пример, что все сработало.
