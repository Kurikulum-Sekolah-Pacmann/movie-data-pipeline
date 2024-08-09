import pyspark
from src.helper.utils import logging_process
from src.transform.process.join_data import join_data_process
from src.transform.process.rename_columns import rename_columns_process
from src.transform.process.select_columns import select_columns_process
from src.transform.process.casting_data import casting_data_process
from src.transform.process.filter_data import filter_data_process
from src.transform.process.feature_engineering import feature_engineering_process
import logging

logging_process()


def transform_movie_data(
    df_ratings: pyspark.sql.DataFrame, df_metadata: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:

    try:
        logging.info("===== Start Transform Movie Data =====")

        df_result = join_data_process(df_ratings=df_ratings,
                                      df_metadata=df_metadata)

        df_result = rename_columns_process(df_result=df_result)

        df_result = select_columns_process(df_result=df_result)

        df_result = casting_data_process(df_result=df_result)

        df_result = filter_data_process(df_result=df_result)

        df_result = feature_engineering_process(df_result=df_result)

        logging.info("===== Finish Transform Movie Data =====")

        return df_result

    except Exception as e:
        logging.error("===== Failed Transform Movie Data =====")
        logging.error(e)

        raise Exception(e)
