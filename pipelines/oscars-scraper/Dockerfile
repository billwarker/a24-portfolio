FROM python:3.12

COPY . .

RUN pip3 install scrapy scrapy-playwright beautifulsoup4

RUN playwright install firefox

COPY . .

CMD [ "python3", "./main.py" ]