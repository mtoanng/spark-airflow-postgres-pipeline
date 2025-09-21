import sys
from pyspark.sql.functions import upper, trim, col
from utils import get_spark_session, write_to_postgres

spark = get_spark_session("CleanCustomers")

customers = spark.read.csv("/home/mtoanng/ecom-analytics/data/raw/olist_customers_dataset.csv", header=True, inferSchema=True)

customers_clean = (customers
    .withColumn("customer_city", trim(col("customer_city")))
    .withColumn("customer_state", trim(col("customer_state")))
    .dropDuplicates(["customer_id"])
)

write_to_postgres(
    customers_clean,
    "dw.dim_customers",
    mode="overwrite"
)
