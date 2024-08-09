from src.extract.extract_data import extract_movie_data
from src.transform.transform_data import transform_movie_data
from src.load.load_data import load_movie_data


if __name__ == "__main__":
    print("===== Start Movie Data Pipeline =====")

    # read data
    df_ratings = extract_movie_data(data_name="ratings", format_data="csv")

    df_metadata = extract_movie_data(data_name="movies_metadata", format_data="db")

    # transform data
    df_result = transform_movie_data(df_ratings=df_ratings, df_metadata=df_metadata)

    # load data
    load_movie_data(df_result=df_result, table_name="obt_rating_movies")

    print("===== Finish Movie Data Pipeline =====")
