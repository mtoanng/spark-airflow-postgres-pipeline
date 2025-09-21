import sys
from utils import get_spark_session, write_to_postgres
from pyspark.sql.functions import col, trim

spark = get_spark_session("CleanSellers")

sellers = spark.read.csv("/home/mtoanng/ecom-analytics/data/raw/olist_sellers_dataset.csv", header=True, inferSchema=True)

sellers_dw = (sellers \
    .withColumn("seller_city", trim(col("seller_city"))) \
    .withColumn("seller_state", trim(col("seller_state"))) \
    .dropDuplicates(["seller_id"])
)

write_to_postgres(
    sellers_dw,
    "dw.dim_sellers",
    mode="overwrite"
)
