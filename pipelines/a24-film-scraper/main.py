import pandas as pd
import numpy as np
import requests as rq
from bs4 import BeautifulSoup
import re
from datetime import datetime

import sys
import time

from google.cloud import bigquery

import os
from flask import Flask

app = Flask(__name__)

def get_film_dict(film_soup):
    film_dict = {}
    film_dict['title'] = film_soup.find('a').get('title')
    film_dict['href'] = film_soup.find('a').get('href')

    if (release_datetime := (film_soup.find('time'))) is not None:
        release_date = datetime.fromisoformat(release_datetime.attrs['datetime'])
        film_dict['release'] = release_date.date()

    elif (release_date := (film_soup.find(attrs={'class':'date'}).text)) is not None:
        film_dict['release'] = datetime.strptime(release_date, '%Y').date()
    
    return film_dict  

def add_film_page_details(film_dict):
    film_page_soup = BeautifulSoup(rq.get(url=film_dict['href']).text, 'html.parser')

    if (film_poster := film_page_soup.find(attrs={'class': 'poster'})) is not None:
        film_dict['poster'] = film_poster.find('a').get('href')
        
    film_dict['synopsis'] = film_page_soup.find(attrs={'class': 'synopsis text-content'}).text.strip()

    film_credits = film_page_soup.find_all(attrs={'class': 'credit'})

    for credit in film_credits:
        if 'directed' in credit.find('h6').text.lower():
            directed_by = credit.find(attrs={'class': 'content'}).text.strip()
            directed_by = directed_by.replace(', and ', ' and ') 
            directed_by = directed_by.replace(' and ', ', ').split(', ')
            
            film_dict['directed_by'] = directed_by
            
        elif credit.find('h6').text == 'Starring':
            starring = credit.find(attrs={'class': 'content'}).text.strip()
            starring = starring.replace(', and ', ' and ') 
            starring = starring.replace(' and ', ', ').split(', ')
            
            film_dict['starring'] = starring

    return film_dict

@app.route("/")
def run(throttle=3, table_id='a24-portfolio.a24.films'):

    a24_films_url = 'https://a24films.com/films'
    payload = {'User-Agent': 'A24 film scraper (williamgeorge.barker@gmail.com)'}

    page = rq.get(url=a24_films_url, params=payload)
    soup = BeautifulSoup(page.text, 'html.parser')

    # films displayed in tile format on page
    tile_films = soup.find_all(attrs={'class': 'media-tile film active has-thumb'})
    # films displayed in list format on page
    list_films = soup.find_all(attrs={'class': 'media-list film active has-thumb'})

    all_films = tile_films + list_films

    all_film_dicts = []

    for ix, film in enumerate(all_films):
        film_dict = get_film_dict(film)
        film_dict['ix'] = ix

        print(f"{film_dict['ix']}, {film_dict['title']}")

        all_film_dicts.append(film_dict)

    for film_dict in all_film_dicts:
        print(film_dict['title'])
        film_dict = add_film_page_details(film_dict)
        print(film_dict)
        print('\n sleeping for 3s \n')
        time.sleep(throttle)

    client = bigquery.Client()

    df = pd.DataFrame(all_film_dicts)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("title", field_type='STRING'),
            bigquery.SchemaField("href", field_type='STRING'),
            bigquery.SchemaField('release', field_type='DATE'),
            bigquery.SchemaField("poster", field_type='STRING'),
            bigquery.SchemaField("synopsis", field_type='STRING'),
            bigquery.SchemaField("directed_by", field_type='STRING', mode='REPEATED'),
            bigquery.SchemaField("starring", field_type='STRING', mode='REPEATED')
        ],
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    
    return job.result()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))