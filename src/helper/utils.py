import logging
from pyspark.sql import SparkSession


def logging_process():
    # Configure logging
    logging.basicConfig(
        filename="/home/jovyan/work/materi/live_class/log/info.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def init_spark_session():
    spark = SparkSession.builder.appName(
        "Live Class Data Pipeline Week 5"
    ).getOrCreate()

    return spark
