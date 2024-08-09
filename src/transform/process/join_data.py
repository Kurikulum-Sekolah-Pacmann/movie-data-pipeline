from src.helper.utils import logging_process
import logging
import pyspark

logging_process()


def join_data_process(
    df_ratings: pyspark.sql.DataFrame, df_metadata: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Function to join two dataframe using PySpark

    Parameters
    ----------
    df_ratings (pyspark.sql.DataFrame): user movies ratings
    df_metadata (pyspark.sql.DataFrame): movies metadata

    Returns
    -------
    df_result (pyspark.sql.DataFrame): joined movies dataframe
    """
    try:
        logging.info("===== Start Joining user ratings and movies metadata data =====")
        df_result = df_ratings.join(
            df_metadata, df_ratings.movieId == df_metadata.id, "inner"
        )

        logging.info("===== Finish Joining user ratings and movies metadata data =====")

        return df_result

    except Exception as e:
        logging.error("===== Failed Joining data =====")
        logging.error(e)

        raise Exception(e)
