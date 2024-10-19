import scrapy
from bs4 import BeautifulSoup
from scrapy_playwright.page import PageMethod

class AcademyAwardsSpider(scrapy.Spider):
    name = "academy-awards-spider"

    def start_requests(self):

        cookies = {
            'cookie_name': 'cookie_value'
        }

        academy_awards_database_url = "https://awardsdatabase.oscars.org/"
        
        yield scrapy.Request(
            url=academy_awards_database_url,
            cookies=cookies,
            meta={
                "playwright": True,  # Enable Playwright for this request
                "playwright_page_methods": [
                            PageMethod("wait_for_timeout", 5000),  # Wait for 5 seconds
                        ]
            },
            callback=self.parse,
        )

    def parse(self, response):
        soup = BeautifulSoup(response.body, 'html.parser')
        pretty_html = soup.prettify()
        print(pretty_html)