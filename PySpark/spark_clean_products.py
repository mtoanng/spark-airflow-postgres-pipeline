import sys
from utils import get_spark_session, write_to_postgres

spark = get_spark_session("CleanProducts")

products = spark.read.csv("/home/mtoanng/ecom-analytics/data/raw/olist_products_dataset.csv", header=True, inferSchema=True)
product_category_name_translation = spark.read.csv("/home/mtoanng/ecom-analytics/data/raw/product_category_name_translation.csv", header=True, inferSchema=True)
products_clean = products.dropDuplicates(["product_id"])
products_clean_translated = products_clean.join(
    product_category_name_translation,
    on="product_category_name",
    how="left"
)

products_final = products_clean_translated.withColumnRenamed(
    "product_category_name_english", "product_category"
)

products_final = products_final.select(
        "product_id",
        "product_category",  # English category name
        "product_name_lenght",
        "product_description_lenght", 
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )
    
write_to_postgres(
    products_final,
    "dw.dim_products",
    mode="overwrite"
)
