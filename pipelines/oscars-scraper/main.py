from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from oscars.spiders.oscars_spider import AcademyAwardsSpider

process = CrawlerProcess(get_project_settings())
process.crawl(AcademyAwardsSpider)
process.start()