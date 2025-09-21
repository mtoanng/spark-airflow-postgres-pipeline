from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2016,1,1),
    'catchup': True
}
import os

spark_env = {
    "SPARK_HOME": "/home/mtoanng/spark-4.0.1-bin-hadoop3",
    "PATH": f"/home/mtoanng/spark-4.0.1-bin-hadoop3/bin:" + os.environ["PATH"],
    "PYSPARK_PYTHON": "/home/mtoanng/airflow_venv/bin/python",
}


SPARK_SUBMIT = "/home/mtoanng/spark-4.0.1-bin-hadoop3/bin/spark-submit"

with DAG(
    'olist_pipeline',
    default_args=default_args,
    schedule_interval='@yearly',
    catchup=True
) as dag:

    t1 = BashOperator(
        task_id='clean_orders',
        bash_command=SPARK_SUBMIT + " /home/mtoanng/ecom-analytics/PySpark/spark_clean_orders.py {{ execution_date.year }}",
        env=spark_env
    )

    t2 = BashOperator(
        task_id='clean_customers',
        bash_command=f'{SPARK_SUBMIT} /home/mtoanng/ecom-analytics/PySpark/spark_clean_customers.py',
        env=spark_env
    )

    t3 = BashOperator(
        task_id='clean_products',
        bash_command=f'{SPARK_SUBMIT} /home/mtoanng/ecom-analytics/PySpark/spark_clean_products.py',
        env=spark_env
    )

    t4 = BashOperator(
        task_id='clean_sellers',
        bash_command=f'{SPARK_SUBMIT} /home/mtoanng/ecom-analytics/PySpark/spark_clean_sellers.py',
        env=spark_env
    )

    t5 = BashOperator(
        task_id='clean_geo',
        bash_command=f'{SPARK_SUBMIT} /home/mtoanng/ecom-analytics/PySpark/spark_clean_geolocation.py',
        env=spark_env
    )
    
    t6 = BashOperator(
        task_id='clean_items',
        bash_command=SPARK_SUBMIT + " /home/mtoanng/ecom-analytics/PySpark/spark_clean_order_items.py {{ execution_date.year }}",
        env=spark_env
    )

    t7 = BashOperator(
        task_id='clean_payments',
        bash_command=SPARK_SUBMIT + " /home/mtoanng/ecom-analytics/PySpark/spark_clean_payments.py {{ execution_date.year }}",
        env=spark_env
    )

    t8 = BashOperator(
        task_id='clean_reviews',
        bash_command=SPARK_SUBMIT + " /home/mtoanng/ecom-analytics/PySpark/spark_clean_reviews.py {{ execution_date.year }}",
        env=spark_env
    )

    t9 = BashOperator(
        task_id='build_fact_sales',
        bash_command=SPARK_SUBMIT + " /home/mtoanng/ecom-analytics/PySpark/spark_build_fact_sales.py {{ execution_date.year }}",
        env=spark_env
    )

    [t2, t3, t4, t5] >> t1 >> [t6, t7, t8] >> t9

