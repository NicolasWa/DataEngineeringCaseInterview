# Task 1: API usage

The task was to build an extract, transform and load data of Star Wars data. The API used is from https://swapi.dev/.

To run this project:
- Create a new virtual environment: python3 -m venv venv
- Activate the virtual environment: source venv/bin/activate
- Install the requirements: pip install -r requirements.txt
- Run the main script: python3 main.py

 main.py will extract, transform and load three different tables (dim_film, dim_people, film_people_map) to a PostgreSQL database.
/!\ You need to input your PostgreSQL database password in the terminal at the beginning of the execution of the program.