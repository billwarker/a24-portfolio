import scrapy
from datetime import date
from academy_awards.items import HonoreeItem

class CeremoniesSpider(scrapy.Spider):

    name = "ceremonies-spider"
    start_urls = [f'https://www.oscars.org/oscars/ceremonies/{x}' for x in range(1930, date.today().year + 1)]
    # start_urls = [f'https://www.oscars.org/oscars/ceremonies/{x}' for x in [1995]]

    def parse(self, response):

        award_categories = response.css(".paragraph--type--award-category")
        
        for category in award_categories:
            category_name = category.css(".field--name-field-award-category-oscars::text").get()

            honorees = category.css(".paragraph--type--award-honoree")

            for honoree in honorees:
                honoree_payload = HonoreeItem()

                honoree_payload['year'] = int(response.url.split('/')[-1])
                honoree_payload['category'] = category_name
                honoree_payload['type'] = honoree.css(".field--name-field-honoree-type::text").get()
                honoree_payload['name'] = honoree.css(".field--name-field-award-entities").css(".field__item::text").get()
                honoree_payload['film'] = honoree.css(".field--name-field-award-film::text").get()

                yield honoree_payload