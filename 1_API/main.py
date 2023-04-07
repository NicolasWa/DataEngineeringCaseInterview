import requests
import json
from postgres_utils import load_data_to_postgresql
import pandas as pd
import hashlib


def get_unique_hash_value(str):
    """Returns a unique integer for the str input."""
    return int(hashlib.sha256(str.encode('utf-8')).hexdigest(), 16)% 10**8

def get_film_dict(row):
    """Returns a dictionnary of all the extracted information for a film."""
    print("extracting: ", row["url"])
    # FILM DICT
    url = row["url"] #get_unique_hash_value(row["url"])  
    title = row["title"]
    episode_id = row["episode_id"]
    opening_crawl = row["opening_crawl"].replace("'", "''")
    director = row["director"]
    producer = row["producer"]
    character_count = len(row["characters"])
    planet_count = len(row["planets"])
    starship_count = len(row["starships"])
    vehicle_count = len(row["vehicles"])
    species_count = len(row["species"])
    release_date = row["release_date"]
    created = row["created"]
    edited = row["edited"]
    
    return {"url": url, "title": title, "episode_id": episode_id, "opening_crawl": opening_crawl, "director": director, "producer": producer, \
            "character_count": character_count, "planet_count": planet_count, "starship_count": starship_count, "vehicle_count": vehicle_count, "species_count": species_count, \
            "release_date": release_date, "created": created, "edited": edited}


def get_mapping_dict(row):
    """Yields the dictionnary mapping a film with a person """
    # MAPPING DICT
    film_url = row["url"]
    for person in row["characters"]:
        person_url = person
        yield {"film_url": film_url, "people_url": person_url}


def get_person_dict(row):
    """Returns a dictionnary of all the extracted information for a person."""
    print("extracting: ", row["url"])
    # Hasing the URL to get a unique integer private key
    url = row["url"]  #id = get_unique_hash_value(row["url"])
    name = row["name"]
    height = row["height"]
    mass = row["mass"]
    hair_color = row["hair_color"]
    skin_color = row["skin_color"]
    eye_color = row["eye_color"]
    birth_year = row["birth_year"]
    gender = row["gender"]
    homeworld_url = row["homeworld"]
    r = requests.get(homeworld_url)
    if r.status_code == 200:
        planet_data = json.loads(r.text)
        homeworld = planet_data["name"]
    else:
        homeworld = "Could not connect, http error"
    species_count = len(row["species"])
    vehicle_count = len(row["vehicles"])
    starship_count = len(row["starships"])
    created = row["created"]
    edited = row["edited"]
    
    return {"url": url, "name": name, "height": height, "mass": mass, "hair_color": hair_color, "skin_color": skin_color, "eye_color": eye_color, \
            "birth_year": birth_year, "gender": gender, "homeworld": homeworld, "species_count": species_count, "vehicle_count": vehicle_count, \
            "starship_count": starship_count, "created": created, "edited": edited}
    

def generator_dimension(dim):
    """Yields dimension entries, one at a time."""
    start_url = f'https://swapi.dev/api/{dim}/'
    url = start_url + "1"
    r = requests.get(url)
    i = 1
    while r.status_code == 200:
        row = json.loads(r.text)
        i+=1
        url = start_url + str(i)
        r = requests.get(url)
        yield row


def extract_data():
    """Returns the 3 pandas DataFrames (films, people, and mapping information) extracted with the API."""
    # Extracting films and mapping
    ls_films = []
    df_mapping = pd.DataFrame({})
    for film in generator_dimension("films"):
        ls_films.append(get_film_dict(film))
        df_mapping = pd.concat([df_mapping, pd.DataFrame.from_records(list(get_mapping_dict(film)))])
    df_films = pd.DataFrame.from_records(ls_films)

    # Extracting people
    ls_people = []
    for person in generator_dimension("people"):
        ls_people.append(get_person_dict(person))
    df_people = pd.DataFrame.from_records(ls_people)
    return df_films, df_people, df_mapping


def transform_data(df_films, df_people, df_film_people):
    """Returns 3 pandas DataFrames (films, people, and mapping information) with clean data."""
    # Cleaning films_people mapping
    df_film_people_mapping = pd.DataFrame({})
    df_film_people_mapping["film_id"] = df_film_people["film_url"].apply(get_unique_hash_value)
    df_film_people_mapping["people_id"] = df_film_people["people_url"].apply(get_unique_hash_value)

    # Cleaning films
    df_films["url"] = df_films["url"].apply(get_unique_hash_value)
    df_films.rename(columns={"url": "id"}, inplace=True)
    df_films.set_index('id', inplace=True)
    df_films = df_films.replace(['', 'n/a', 'unknown'], None)
    df_films['created'] = pd.to_datetime(df_films['created']).dt.tz_convert(None)
    df_films['edited'] = pd.to_datetime(df_films['edited']).dt.tz_convert(None)

    # Cleaning people
    df_people["url"] = df_people["url"].apply(get_unique_hash_value)
    df_people.rename(columns={"url": "id"}, inplace=True)
    df_people.set_index('id', inplace=True)
    df_people['created'] = pd.to_datetime(df_people['created']).dt.tz_convert(None)
    df_people['edited'] = pd.to_datetime(df_people['edited']).dt.tz_convert(None)
    df_people['mass'] = df_people['mass'].apply(lambda x: x.replace(",", ""))
    df_people = df_people.replace(['', 'n/a', 'unknown'], None)
    df_people['mass'] = df_people['mass'].fillna(pd.NA).astype("Int64")
    df_people['height'] = df_people['height'].fillna(pd.NA).astype("Int64")
    
    return df_films, df_people, df_film_people_mapping


if __name__ == '__main__':
    """This program extracts, transforms and loads data of Star Wars data into a PostgreSQL database. 
    The API used to extract the data is from https://swapi.dev/.
    """
     # DB variables
    host = "localhost"
    user = "postgres"
    password = input("Input your postgres password: (required to load the data into the database): ")
    database_name = "starwars"

    table_films_name = "dim_film"
    table_people_name = "dim_people"
    table_film_people_name = "film_people_map"

    # EXTRACT
    df_films_extracted, df_people_extracted, df_film_people_map = extract_data()

    # TRANSFORM
    df_films_cleaned, df_people_cleaned, df_film_people_map_cleaned = transform_data(df_films_extracted, df_people_extracted, df_film_people_map)
    
    # LOAD
    load_data_to_postgresql(df_films_cleaned, database_name, table_films_name, user, password, host)
    load_data_to_postgresql(df_people_cleaned, database_name, table_people_name, user, password, host)
    load_data_to_postgresql(df_film_people_map_cleaned, database_name, table_film_people_name, user, password, host)
