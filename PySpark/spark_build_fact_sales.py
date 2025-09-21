import sys
from utils import get_spark_session, write_to_postgres
from pyspark.sql.functions import sum, avg, count, first

spark = get_spark_session("BuildFactSales")

execution_year = int(sys.argv[1])

orders = spark.read.parquet("/home/mtoanng/ecom-analytics/data/staging/orders") \
    .filter(f"order_year = {execution_year}") \
    .select("order_id","customer_id","order_date","order_year", "order_month")
    
items = spark.read.parquet("/home/mtoanng/ecom-analytics/data/staging/order_items_enriched") \
    .filter(f"order_year = {execution_year}") \
    .select("order_id","price","freight_value") \
    .groupBy("order_id") \
    .agg(
        sum("price").alias("total_price"),
        sum("freight_value").alias("total_freight_value"),
        count("order_id").alias("item_count")
    )

payments = spark.read.parquet("/home/mtoanng/ecom-analytics/data/staging/payments_enriched") \
    .filter(f"order_year = {execution_year}") \
    .select("order_id","payment_value") \
    .groupBy("order_id") \
    .agg(
        sum("payment_value").alias("total_payment_value"),
        count("order_id").alias("payment_method_count")
    )


reviews = spark.read.parquet("/home/mtoanng/ecom-analytics/data/staging/reviews_enriched") \
    .filter(f"order_year = {execution_year}") \
    .select("order_id","review_score") \
    .groupBy("order_id") \
    .agg(
        avg("review_score").alias("review_score"),
    )

fact_sales = orders.join(items,"order_id","left") \
    .join(payments,"order_id","left") \
    .join(reviews,"order_id","left")

fact_sales.write.partitionBy("order_year","order_month") \
    .mode("append") \
    .parquet("/home/mtoanng/ecom-analytics/data/staging/fact_sales")

fact_sales=fact_sales.select(
        orders.order_id, orders.customer_id, orders.order_date,
        items.total_price, items.total_freight_value, items.item_count,
        payments.total_payment_value, payments.payment_method_count,
        reviews.review_score
    )

write_to_postgres(fact_sales, "dw.fact_sales", mode="append")

