from src.helper.utils import logging_process
import logging
import pyspark

logging_process()


def filter_data_process(
    df_result: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Function that used to filter the data based on the conditions

    Parameters
    ----------
    df_result (pyspark.sql.DataFrame): full movie data

    Returns
    -------
    df_result (pyspark.sql.DataFrame): filtered movie data
    """
    try:
        logging.info("===== Start Filter Data =====")

        df_result = df_result.filter(
            "release_date >= '2010-01-01' AND timestamp >= '2017-01-01 00:00:00'"
        )

        logging.info("===== Finish Filter Data =====")

        return df_result

    except Exception as e:
        logging.error("===== Failed Filter Data =====")
        logging.error(e)
        raise Exception(e)
