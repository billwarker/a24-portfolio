from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy_project.spiders.ceremonies_spider import CeremoniesSpider

process = CrawlerProcess(get_project_settings())
process.crawl(CeremoniesSpider)
process.start()