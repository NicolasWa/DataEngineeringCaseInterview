import os
from scrapy import cmdline
import sys

data_file_name = "movies.json"
try:
    args = sys.argv
    data_file_name = args[1]
except:
    pass
#cmdline.execute(["scrapy", "crawl", "top_movies", "-O", "movies_new.json"])
cmdline.execute(f"scrapy crawl top_movies -O {data_file_name}".split())