from pyspark.sql import SparkSession

def get_spark_session(app_name="OlistPipeline"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

def write_to_postgres(df, table_name, mode="append"):
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://localhost:5432/olist") \
      .option("dbtable", table_name) \
      .option("user", "postgres") \
      .option("password", "12345") \
      .option("driver", "org.postgresql.Driver") \
      .mode(mode) \
      .save()

def close_spark_session():
    spark = SparkSession.getActiveSession()
    if spark:
        spark.stop()
