from src.helper.utils import logging_process
import logging
import pyspark

logging_process()


def select_columns_process(
    df_result: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Function that used to selecting data based on columns using list

    Parameters
    ----------
    df_result (pyspark.sql.DataFrame): full movies pyspark dataframe

    Returns
    -------
    df_result (pyspark.sql.DataFrame): selected movies pyspark dataframe
    """
    try:
        logging.info("===== Start Selecting Data process =====")

        SELECTED_COLS = [
            "user_id",
            "movie_id",
            "rating",
            "timestamp",
            "adult",
            "budget",
            "genres",
            "original_language",
            "original_title",
            "overview",
            "popularity",
            "production_companies",
            "release_date",
            "revenue",
            "runtime",
            "spoken_languages",
            "status",
            "tagline",
            "title",
            "vote_average",
            "vote_count",
        ]

        df_result = df_result.select(SELECTED_COLS)

        logging.info("===== Finish Selecting Data process =====")

        return df_result

    except Exception as e:
        logging.error("===== Failed Selecting Data process =====")
        logging.error(e)

        raise Exception(e)
