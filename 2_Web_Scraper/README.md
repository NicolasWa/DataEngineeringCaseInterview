# Task 2: Web Scraper

The task was to extract, transform and load (ETL) the data from the first page of the Most Popular Movies at Home in Rotten Tomatoes via web scraping (https://www.rottentomatoes.com/browse/movies_at_home/sort:popular?page=1).
The python package used in this project is Scrapy.

To run this project:
- Create a new virtual environment: python3 -m venv venv
- Activate the virtual environment: source venv/bin/activate
- Install the requirements: pip install -r requirements.txt
- Run the main program: python3 main.py

 main.py will extract the data (web scraping using scrapy), transform it and load it (ETL) to a PostgreSQL database (a new database 'web_scraping_rotten_tomatoes' will be created and a new table 'fact_top_movies' inside it).
/!\ You need to input your PostgreSQL database password in the terminal at the beginning of the execution of the program.