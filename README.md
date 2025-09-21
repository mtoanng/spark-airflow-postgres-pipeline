🛒 E-commerce Analytics Pipeline (Olist Dataset)
📖 Overview
This project simulates a real-world Data Engineering pipeline for the Olist e-commerce dataset:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_order_items_dataset.csv

The pipeline demonstrates:
- Batch data ingestion from raw CSV snapshots
- Data cleaning and enrichment with Apache Spark
- Partitioning strategy to optimize performance for large-scale datasets
- Data Warehouse design (Star Schema) with dimensions and fact tables
- Orchestration of ETL jobs using Apache Airflow
- Loading into PostgreSQL for BI / analytics consumption

The project is designed not only to produce working analytics but also to showcase scalable thinking, such as partitioned storage, yearly batch runs, and modular DAG orchestration.


🏗️ Architecture
      +---------+        +------------------+        +-----------------+       +-------------+
      |  Raw    | -----> |  Staging (Spark) | -----> | Data Warehouse  | ----> | BI / Reports|
      |  CSV    |        | (Cleaned Parquet)|        | (Postgres Star) |       | PowerBI etc |
      +---------+        +------------------+        +-----------------+       +-------------+

- Raw Layer: Original Olist CSV datasets
- Staging Layer: Cleaned data stored in Parquet, partitioned by year for efficient batch processing
- Warehouse Layer: Star Schema in PostgreSQL containing:
    + Dimension tables (dim_customers, dim_products, dim_sellers)
    + Fact tables (fact_orders, fact_payments, fact_reviews, fact_order_items)
- Aggregated fact_sales fact built from other fact tables
- Airflow: Orchestrates end-to-end jobs on a daily schedule (simulating batch processing, but configured for yearly partition loads)
- Spark: Handles heavy ETL (deduplication, enrichment, partitioning, joins)


⚙️ Setup
1. Requirements
- Python 3.9+
- Java 11 (required for Spark)
- PostgreSQL 14+
- Apache Spark 3.5+
- Apache Airflow 2.9+

2. Install dependencies
pip install -r requirements.txt

3. Install Spark & Postgres JDBC driver
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar xvf spark-3.5.0-bin-hadoop3.tgz
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark

# Add to PATH
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=$SPARK_HOME/bin:$PATH" >> ~/.bashrc
source ~/.bashrc

# PostgreSQL JDBC driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P /opt/spark/jars/

4. Initialize database schemas
createdb olist

psql -U postgres -d olist -f sql/dw.sql

5. Initialize Airflow
airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin


🚀 Running the Pipeline
1. Start Airflow
./shell/start.sh

2. Trigger batch jobs (2016–2018)
airflow dags backfill -s 2016-01-01 -e 2018-12-31 olist_pipeline

This will run 3 yearly partitions:
- Batch for 2016
- Batch for 2017
- Batch for 2018

3. Validate data in PostgreSQL



👤 Author
Nguyen Dung Manh Toan



