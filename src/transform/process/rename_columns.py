from src.helper.utils import logging_process
import logging
import pyspark

logging_process()


def rename_columns_process(
    df_result: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Functions that used to renaming columns based on the requirements

    Parameters
    ----------
    df_result (pyspark.sql.DataFrame): pyspark movies dataframe

    Returns
    -------
    df_resultv (pyspark.sql.DataFrame): final result pyspark movies dataframe that already renamed
    """
    try:
        logging.info("===== Start Renaming Columns based on the requirements =====")
        RENAME_COLS = {"userId": "user_id", "movieId": "movie_id"}

        df_result = df_result.withColumnsRenamed(colsMap=RENAME_COLS)

        logging.info("===== Finish Renaming Columns based on the requirements =====")

        return df_result

    except Exception as e:
        logging.error("===== Failed Renaming Columns based on the requirements =====")
        logging.error(e)

        raise Exception(e)
