import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd

def connect_default_database(db_host, db_user, db_password, auto_commit=True):
    """Returns a cursor and a connection for the default postgres database."""
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
    """Returns a cursor and a connection for the database 'db_name'."""
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


def load_data_to_postgresql(new_data_df, db_name, t_name, user_name, passw, host_name):
    """Loads the pandas DataFrame 'new_data_df' into a postgresql database 'db_name'.
     The existing content of the database is first loaded to check for duplicates,
     and the data (without duplicates) is overwritten on the database (replace)."""
    # Creating a the database it it doesn't exist yet
    cur, conn = connect_default_database(host_name, user_name, passw)
    try:
        cur.execute(f"CREATE DATABASE {db_name}")
    except psycopg2.Error as e:
        # The database already exists, no need to create it
        pass

    cur.close()
    conn.close()

    # Connection to the DB
    engine = create_engine(f'postgresql://{user_name}:{passw}@{host_name}/{db_name}') 
    dbConn = engine.connect()
    # Loading the content of the database
    try:
        existing_data_df = pd.read_sql(text(f"SELECT * FROM {t_name}"), dbConn)
        existing_data_df = existing_data_df.drop(columns=['index'])
    except:
        # The table didn't exist yet, so there is no existing data
        existing_data_df = pd.DataFrame({})
        pass

    dbConn.close()

    # Removing duplicates
    df_no_duplicates = pd.concat([existing_data_df, new_data_df]).drop_duplicates()
    df_no_duplicates.to_sql(t_name, engine, if_exists='replace')
    engine.dispose()
    print(df_no_duplicates)
