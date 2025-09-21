import sys
from utils import get_spark_session, write_to_postgres

spark = get_spark_session("CleanItems")

items = spark.read.csv("/home/mtoanng/ecom-analytics/data/raw/olist_order_items_dataset.csv", header=True, inferSchema=True)

execution_year = int(sys.argv[1])

orders = spark.read.parquet("/home/mtoanng/ecom-analytics/data/staging/orders").filter(f"order_year = {execution_year}")

items_enriched = items.join(
    orders.select("order_id","order_year","order_month"),
    on="order_id",
    how="inner"
)

items_enriched.write.partitionBy("order_year","order_month") \
    .mode("append") \
    .parquet("/home/mtoanng/ecom-analytics/data/staging/order_items_enriched")

write_to_postgres(
    items_enriched.select("order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"),
    "dw.fact_order_items",
    mode="append"
)

