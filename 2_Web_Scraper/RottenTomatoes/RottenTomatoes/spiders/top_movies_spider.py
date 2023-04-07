import scrapy


class QuotesSpider(scrapy.Spider):
    name = "top_movies"
    start_urls = [
        'https://www.rottentomatoes.com/browse/movies_at_home/sort:popular?page=1',
    ]

    def parse(self, response):
        base_url = 'https://www.rottentomatoes.com/'
        films = response.css('.discovery-tiles__wrap .js-tile-link')
        all_hrefs = response.css('.discovery-tiles__wrap .js-tile-link').css('a::attr(href)').getall()
        movie_page_links = [base_url + movie_href for movie_href in all_hrefs]
        yield from response.follow_all(movie_page_links, self.parse_movie)


    def parse_movie(self, response):
        def process_score_board(score_board_resp):
            title, nb_reviews, tomato_score, audience_score = ["" for i in range(4)]
            title = score_board_resp.css('.scoreboard__title::text').get()
            nb_reviews = score_board_resp.css('.scoreboard__link.scoreboard__link--tomatometer::text').get().strip()
            tomato_score = score_board_resp.css('score-board::attr(tomatometerscore)').get()
            audience_score = score_board_resp.css('score-board::attr(audiencescore)').get()
            return title, nb_reviews, tomato_score, audience_score
            
        def process_movie_info(movie_info_section_resp):
            synopsis, rating, genre, language, director, producer, writer, release_date_theaters, release_date_streaming, \
                runtime, distributor, aspect_ratio = ["" for i in range(12)]
            synopsis = movie_info_section_resp.css('p::text').get().strip()
            items_info_section = movie_info_section_resp.css('#info .info-item')
            for item in items_info_section:
                label = item.css('.info-item-label::text').get()
                if label == "Rating:":
                    rating = item.css('.info-item-value::text').get().strip()
                elif label == "Genre:":
                    genre = item.css('.info-item-value::text').get().strip().replace("  ", "").replace("\n", "")
                elif label == "Original Language:":
                    language = item.css('.info-item-value::text').get().strip()
                elif label == "Director:":
                    director = item.css('.info-item-value a::text').get().strip()
                elif label == "Producer:":
                    producer = item.css('.info-item-value a::text').get().strip()
                elif label == "Writer:":
                    writer = item.css('.info-item-value a::text').get().strip()
                elif label == "Release Date (Theaters):":
                    release_date_theaters = item.css('.info-item-value time::text').get().strip()
                elif label == "Release Date (Streaming):":
                    release_date_streaming = item.css('.info-item-value time::text').get().strip()
                elif label == "Runtime:":
                    runtime = item.css('.info-item-value time::text').get().strip().replace(" ", "")
                elif label == "Distributor:":
                    distributor = item.css('.info-item-value::text').get().strip().replace("  ", "").replace("\n", "")
                elif label == "Aspect Ratio:":
                    aspect_ratio = item.css('.info-item-value::text').get().strip()
            #TODO: add movie_production_co but what's his label? can't find it
            return synopsis, rating, genre, language, director, producer, writer, release_date_theaters, release_date_streaming, runtime, distributor, aspect_ratio
        

        movie_url = response.url
        score_board = response.css('.thumbnail-scoreboard-wrap')
        movie_title, movie_nb_reviews, movie_tomato_score, movie_audience_score = process_score_board(score_board)
        movie_info_section = response.css('.media-body .panel-body.content_body')
        movie_synopsis, movie_rating, movie_genre, movie_language, movie_director, movie_producer, movie_writer, \
        movie_release_date_theaters, movie_release_date_streaming, movie_runtime, movie_distributor, movie_aspect_ratio \
            = process_movie_info(movie_info_section)

        #TODO: add this to the process_info
        movie_production_co = ""
        yield {
            'title': movie_title,
            'reviews': movie_nb_reviews,
            'tomatometer': movie_tomato_score,
            'audience_score': movie_audience_score,
            'synopsis': movie_synopsis,
            'rating': movie_rating,
            'genre': movie_genre,
            'original_language': movie_language,
            'director': movie_director,
            'producer': movie_producer,
            'writer': movie_writer,
            'release_date_theaters': movie_release_date_theaters,
            'release_date_streaming': movie_release_date_streaming,
            'runtime_hours': movie_runtime,
            'distributor': movie_distributor,
            'production_co': movie_production_co,
            'aspect_ratio': movie_aspect_ratio,
            'url': movie_url
        }

