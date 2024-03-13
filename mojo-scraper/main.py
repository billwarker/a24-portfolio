import pandas as pd
import numpy as np
import requests as rq
from bs4 import BeautifulSoup
import re

import sys
import time

from google.cloud import bigquery

client = bigquery.Client()

def run_query_in_bq(client, query: str):
    
    # Perform a query.
    query_job = client.query(query) # API request

    return query_job.result()

def write_df_to_bq(client, df, schema, dataset_id, table_name):

    table_id = f"{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
        )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
        )  # Make an API request.

    return job.result()

def mojo_search_for_release_url(title: str, release_year: int, sleep_interval=5) -> str:

    print(f'Title: {title}, {release_year}')

    match_url = None
    release_url = None

    mojo_root_url = 'https://www.boxofficemojo.com'
    mojo_search_ext = '/search/?q='
    
    qsp = title.lower().replace(' ', '+')

    time.sleep(sleep_interval)

    search_page = rq.get(url=mojo_root_url + mojo_search_ext + qsp)
    soup = BeautifulSoup(search_page.text, 'html.parser')

    for search_result in soup.find_all(attrs={'class': 'a-fixed-left-grid'}):
        
        result_name = search_result.find(attrs={'class': 'a-size-medium a-link-normal a-text-bold'})
        result_year = int(re.search('\(([0-9]{4})\)', search_result.find(attrs={'class': 'a-color-secondary'}).text).group(1))
    
        print(f'Search Result: {result_name.text}, {result_year}')

        if title.lower() == result_name.text.lower() and (abs(release_year - result_year) in (0, 1)):
            
            match_url = result_name['href']
            print(f'Match Found: {mojo_root_url + match_url}')
            
            break

    if match_url:
        
        time.sleep(sleep_interval)

        match_page = rq.get(url='https://www.boxofficemojo.com' + match_url)
        match_soup = BeautifulSoup(match_page.text, 'html.parser')

        for link in match_soup.find_all('a'):
            
            if '/release/' in link['href']:
                
                release_url_ext = re.search('(\/release\/rl[0-9]+)\/.*$', link['href']).group(1)
                release_url = mojo_root_url + release_url_ext
                print(f'Release Found: {release_url}')
                     
                break

        if not release_url:
            print('No Release Found')
        
    else:
        print('No Match Found')
    
    return release_url

def mojo_film_search():
    query = """
    SELECT title, EXTRACT(YEAR FROM release_date) AS release_year
    FROM `a24-portfolio.wikipedia.list_of_A24_films`
    WHERE
        EXTRACT(YEAR FROM release_date) >= 2022
    
    ORDER BY release_date
    LIMIT 5
    """

    client = bigquery.Client()

    print(f"""Running query in BQ:
          
          ---
          {query}
          ---
          """)

    query_results = run_query_in_bq(client, query)

    print('Searching boxofficemojo.com for films...\n')

    data = {}

    for ix, row in enumerate(query_results):
        title = row.title
        release_year = int(row.release_year)

        mojo_release_url = mojo_search_for_release_url(title, release_year)

        data[ix] = {
            'title': title,
            'release_year': release_year,
            'mojo_release_url': mojo_release_url
            }
        
        print(f'\nData collected: {data[ix]}\n')
    
    df = pd.DataFrame.from_dict(data, orient='index')

    return df

if __name__ == '__main__':

    df = mojo_film_search()
    print(df)