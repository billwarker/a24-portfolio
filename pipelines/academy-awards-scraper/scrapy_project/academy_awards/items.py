# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AcademyAwardsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class HonoreeItem(scrapy.Item):
    year = scrapy.Field()
    category = scrapy.Field()
    name = scrapy.Field()
    type = scrapy.Field()
    film = scrapy.Field()
    pass
