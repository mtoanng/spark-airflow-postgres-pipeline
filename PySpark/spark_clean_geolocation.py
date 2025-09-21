import sys
from utils import get_spark_session, write_to_postgres
from pyspark.sql.functions import udf, avg
from pyspark.sql.types import StringType
import unicodedata

def normalize_text(s):
    if s is None:
        return None
    nfkd = unicodedata.normalize('NFKD', s)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

normalize_udf = udf(normalize_text, StringType())

spark = get_spark_session("CleanGeolocation")

geo = spark.read.csv("/home/mtoanng/ecom-analytics/data/raw/olist_geolocation_dataset.csv",
                     header=True, inferSchema=True)

geo = geo.withColumn("geolocation_city", normalize_udf(geo["geolocation_city"]))

geo_clean = geo.groupBy("geolocation_zip_code_prefix", "geolocation_city", "geolocation_state") \
               .agg(avg("geolocation_lat").alias("latitude"),
                    avg("geolocation_lng").alias("longitude")) \
               .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix") \
               .withColumnRenamed("geolocation_city", "city") \
               .withColumnRenamed("geolocation_state", "state")

write_to_postgres(
    geo_clean,
    "dw.dim_geography",
    mode="overwrite"
)

