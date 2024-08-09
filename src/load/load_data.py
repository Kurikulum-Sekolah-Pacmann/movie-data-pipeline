from src.helper.utils import logging_process
import logging
import pyspark

logging_process()


def load_movie_data(df_result: pyspark.sql.DataFrame, table_name: str) -> None:
    """
    Function that used to dump the result to the database using PySpark

    Parameters
    ----------
    df_result (pyspark.sql.DataFrame): final result of pyspark movie dataframe
    """
    try:
        # set variable for database
        DB_URL = "jdbc:postgresql://movie_db_container:5432/movie"
        DB_USER = "postgres"
        DB_PASS = "cobapassword"

        # set config
        jdbc_url = DB_URL
        connection_properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "org.postgresql.Driver",  # set driver postgres
        }

        logging.info("===== Start Load data to the database =====")

        # load data
        df_result.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=connection_properties,
        )

        logging.info("===== Finish Load data to the database =====")

    except Exception as e:
        logging.error("===== Failed Load data to the database =====")
        logging.error(e)
        raise Exception(e)
