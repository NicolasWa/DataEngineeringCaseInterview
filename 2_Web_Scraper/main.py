import os
import subprocess
import pandas as pd
from postgres_utils import load_data_to_postgresql


def convert_hours_to_min(time_hours_str):
    """Returns the amount of time expressed by 'time_hours_str' in minutes."""
    index_h = time_hours_str.find("h")
    index_m = time_hours_str.find("m")
    nb_hours = int(time_hours_str[:index_h])
    nb_min = int(time_hours_str[index_h+1:index_m])
    return nb_hours * 60 + nb_min

def transform_reviews(reviews_str):
    """Returns the number of reviews (integer) extracted from 'reviews_str'."""
    space_index = reviews_str.find(" ")
    int_reviews = int(reviews_str[:space_index])
    return int_reviews

def convert_percentage_to_float(x):
    """Returns the percentage, expressed by a number between 0 and 1, 
    from an integer input ranging from 0 to 100
    """
    return x / 100


def clean_dataframe(df):
    """Returns the pandas DataFrame given in input with clean data."""
    # Numbers
    df["audience_score"] = df["audience_score"].apply(convert_percentage_to_float)
    df["tomatometer"] = df["tomatometer"].apply(convert_percentage_to_float)
    df["runtime_hours"] = df["runtime_hours"].apply(convert_hours_to_min)
    df.rename(columns={"runtime_hours": "runtime_mins"}, inplace=True)
    df["reviews"] = df["reviews"].apply(transform_reviews)
    # Dates
    df['release_date_theaters'] = pd.to_datetime(df['release_date_theaters'])
    df['release_date_streaming'] = pd.to_datetime(df['release_date_streaming'])
    # (release) 'year' field is a Datetime converted to integer. 
    # If release date in theatres is missing, then 'year' defautlt to release date on the streaming platform
    df["year"] = df["release_date_theaters"].where(df["release_date_theaters"]==pd.NaT, df["release_date_theaters"].dt.year)
    df["year"] = df["year"].fillna(df["release_date_streaming"].dt.year)
    df["year"] = df["year"].fillna(pd.NA).astype("Int64")  # fills missing values with NAs

    # Updating the empty strings to None (will automatically be set to 'NULL' when inserting in the Postgres table) 
    df = df.replace([""], None)

    return df


if __name__ == "__main__":
    """This program :
    - Scrapes the RottenTomatoes 'Most Popular Movies at Home' webpage (https://www.rottentomatoes.com/browse/movies_at_home/sort:popular?page=1) using Scrapy
    - Transforms (cleans) the data
    - Loads it into a PostgreSQL database
    """
    # Defining the paths
    scrapy_folder_path = os.path.join(os.getcwd(), "RottenTomatoes")
    name_file = "movies.json"
    data_path = os.path.join(scrapy_folder_path, name_file)

    # Scraping the website (EXTRACT)
    subprocess.run(["python3", "launch_crawler.py", name_file], capture_output=True, text=True, cwd=scrapy_folder_path)  #, cwd="./RottenTomatoes/"

    # Cleaning the data (TRANSFORM)
    df = pd.read_json(data_path)
    df_cleaned = clean_dataframe(df)
    
    # Loading it to a postgresql database (LOAD)
    user = "postgres"
    password = input("Insert your posgres password (required to load the data to the database): ")
    host = "localhost"
    db_name = "web_scraping_rotten_tomatoes"
    table_name = "fact_top_movies"
    load_data_to_postgresql(df_cleaned, db_name, table_name, user, password, host)






