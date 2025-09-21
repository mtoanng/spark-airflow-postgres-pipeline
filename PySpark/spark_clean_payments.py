import sys
from utils import get_spark_session, write_to_postgres

spark = get_spark_session("CleanPayments")

payments = spark.read.csv("/home/mtoanng/ecom-analytics/data/raw/olist_order_payments_dataset.csv", header=True, inferSchema=True)

execution_year = int(sys.argv[1])

orders = spark.read.parquet("/home/mtoanng/ecom-analytics/data/staging/orders").filter(f"order_year = {execution_year}")

payments_enriched = payments.join(
    orders.select(
        "order_id",
        "order_purchase_timestamp",
        "order_year",
        "order_month"),
    on="order_id",
    how="inner"
)

payments_enriched.write.partitionBy("order_year","order_month") \
    .mode("append") \
    .parquet("/home/mtoanng/ecom-analytics/data/staging/payments_enriched")
    
payments_dw=payments_enriched.select(
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
    "order_purchase_timestamp"
)

write_to_postgres(
    payments_dw,
    "dw.fact_payments",
    mode="append"
)
