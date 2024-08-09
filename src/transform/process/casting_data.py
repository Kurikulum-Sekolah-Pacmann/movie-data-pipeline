from src.helper.utils import logging_process
from pyspark.sql.functions import from_unixtime
import logging
import pyspark

logging_process()


def casting_data_process(
    df_result: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Function that used to casting data type
    by passing dictionary to pyspark method,
    also convert unix time to timestamp using pyspark functions

    Parameters
    ----------
    df_result (pyspark.sql.DataFrame): movies data that we want to cast

    Returns
    -------
    df_result (pyspark.sql.DataFrame): movies data that already been cast
    """
    try:
        logging.info("===== Start Casting Data =====")

        CASTING_COLS = {
            "user_id": "int",
            "movie_id": "int",
            "budget": "float",
            "popularity": "float",
            "revenue": "float",
            "runtime": "float",
            "vote_average": "float",
            "vote_count": "float",
        }

        for col_name, data_type in CASTING_COLS.items():
            df_result = df_result.withColumn(
                col_name, df_result[col_name].cast(data_type)
            )

        df_result = df_result.withColumn(
            "timestamp", from_unixtime("timestamp").alias("ts")
        )

        logging.info("===== Finish Casting Data =====")

        return df_result

    except Exception as e:
        logging.error("===== Failed Casting Data =====")
        logging.error(e)

        raise Exception(e)
