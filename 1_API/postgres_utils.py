import psycopg2
from sqlalchemy import create_engine

def connect_default_database(db_host, db_user, db_password, auto_commit=True):
    default_db_name = "postgres"
    try:
        conn = psycopg2.connect(f"host={db_host} dbname={default_db_name} user={db_user} password={db_password}")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: could not get cursor to the DB")
        print(e)
    conn.set_session(autocommit=auto_commit)
    return cur, conn

def connect_to_database(db_host, db_user, db_password, db_name):
    try:
        con = psycopg2.connect(f"host={db_host} dbname={db_name} user={db_user} password={db_password}")
    except psycopg2.Error as e:
        print(f"Error: Could not make connection to the {db_name} database")
        print(e)
    con.set_session(autocommit=True)

    try:
        cur = con.cursor()
    except psycopg2.Error as e:
        print("Error: could not get cursor to the DB")
        print(e)

    return cur, con

def create_database(db_host, db_user, db_password, db_name):
    # Connection to default DB in order to be able to create the one we want
    cur, conn = connect_default_database(db_host, db_user, db_password)
    # Creation of a first database to work with
    try:
        cur.execute(f"CREATE DATABASE {db_name}")
    except psycopg2.Error as e:
        print("Error: could not create database")
        print(e)
     # closing connection to default DB
    conn.close()
    cur.close()
    return None


def load_data_to_postgresql(df, db_name, t_name, user_name, passw, host_name):
    # Creating a the database
    cur, conn = connect_default_database(host_name, user_name, passw)
    try:
        cur.execute(f"CREATE DATABASE {db_name}")
    except psycopg2.Error as e:
        # The database already exists, no need to create it
        pass

    cur.close()
    conn.close()

    # Inserting the cleaned data into the postgresql database
    engine = create_engine(f'postgresql://{user_name}:{passw}@{host_name}/{db_name}')
    df.to_sql(t_name, engine, if_exists='replace')
    engine.dispose()