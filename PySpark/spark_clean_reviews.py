import sys
from utils import get_spark_session, write_to_postgres
from pyspark.sql.functions import col, expr 

spark = get_spark_session("CleanReviews")

reviews = spark.read.csv("/home/mtoanng/ecom-analytics/data/raw/olist_order_reviews_dataset.csv", header=True, inferSchema=True)

reviews = reviews \
    .withColumn("review_creation_date", expr("try_to_timestamp(review_creation_date, 'yyyy-MM-dd HH:mm:ss')")) \
    .withColumn("review_answer_timestamp", expr("try_to_timestamp(review_answer_timestamp, 'yyyy-MM-dd HH:mm:ss')")) \
    .withColumn("review_score", col("review_score").cast("int"))
    
execution_year = int(sys.argv[1])

orders = spark.read.parquet("/home/mtoanng/ecom-analytics/data/staging/orders").filter(f"order_year = {execution_year}")

reviews_enriched = reviews.join(
    orders.select(
        "order_id",
        "order_year",
        "order_month"
    ),
    on="order_id",
    how="inner"
)

reviews_enriched=reviews_enriched.dropDuplicates(["review_id"])

reviews_enriched.write.partitionBy("order_year","order_month") \
    .mode("append") \
    .parquet("/home/mtoanng/ecom-analytics/data/staging/reviews_enriched")
    
reviews_dw=reviews_enriched.select(
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp"
)
    
write_to_postgres(
    reviews_dw,
    "dw.fact_reviews",
    mode="append"
)
