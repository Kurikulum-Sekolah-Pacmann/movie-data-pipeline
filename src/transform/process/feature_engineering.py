from src.helper.utils import logging_process
import logging
import pyspark

logging_process()


def feature_engineering_process(
    df_result: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Function that used to create a new features by using existing features

    Parameters
    ----------
    df_result (pyspark.sql.DataFrame): movies dataframe

    Returns
    -------
    df_result (pyspark.sql.DataFrame): movies dataframe with new columns
    """
    try:
        logging.info(
            "===== Start creating new features by using existing features ====="
        )

        df_result = df_result.withColumn(
            "profit", df_result["revenue"] - df_result["budget"]
        )

        logging.info(
            "===== Finish creating new features by using existing features ====="
        )

        return df_result

    except Exception as e:
        logging.error(
            "===== Failed creating new features by using existing features ====="
        )
        raise Exception(e)
