import sys
from utils import get_spark_session, write_to_postgres
from pyspark.sql.functions import col, to_date, year, month

spark = get_spark_session("CleanOrders")

orders = spark.read.csv("/home/mtoanng/ecom-analytics/data/raw/olist_orders_dataset.csv", header=True, inferSchema=True)

execution_year = int(sys.argv[1])

orders_clean = orders.dropDuplicates(["order_id"]) \
    .withColumn("order_date", to_date("order_purchase_timestamp")) \
    .withColumn("order_year", year("order_purchase_timestamp"))
    
orders_clean = orders_clean \
    .filter(col("order_year") == execution_year) \
    .withColumn("order_month", month("order_purchase_timestamp"))


orders_clean.write.partitionBy("order_year","order_month") \
    .mode("append") \
    .parquet("/home/mtoanng/ecom-analytics/data/staging/orders")
    
orders_dw=orders_clean.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
)
write_to_postgres(
    orders_dw,
    "dw.fact_orders",
    mode="append"
)

