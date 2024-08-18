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

def write_df_to_bq(client, df, schema_path, dataset_id, table_name):

    client.create_dataset(dataset_id, exists_ok=True)

    table_id = f"{dataset_id}.{table_name}"
    
    schema = client.schema_from_json(schema_path)

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

def mojo_search_for_film_release(title: str,
                                 release_year: int,
                                 sleep_interval=3) -> str:

    print(f'Title: {title}, {release_year}')

    match_url = None

    release_data = {
        'mojo_title': None,
        'mojo_release_year': None,
        'mojo_release_url': None
        }

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

            release_data['mojo_title'] = result_name.text
            release_data['mojo_release_year'] = result_year
            
            break

    if match_url:
        
        time.sleep(sleep_interval)

        match_page = rq.get(url='https://www.boxofficemojo.com' + match_url)
        match_soup = BeautifulSoup(match_page.text, 'html.parser')

        mojo_release_url = None

        for link in match_soup.find_all('a'):
            
            if '/release/' in link['href']:
                
                release_url_ext = re.search('(\/release\/rl[0-9]+)\/.*$', link['href']).group(1)
                mojo_release_url = mojo_root_url + release_url_ext
                print(f'Release Found: {mojo_release_url}')
                release_data['mojo_release_url'] = mojo_release_url

                break

        if not mojo_release_url:
            print('No Release Found')
    
    else:
        print('No Match Found')

    return release_data

def mojo_film_search(
        query_path, 
        schema_path,
        dataset_id,
        table_name,
        sleep_interval):

    print('Running mojo-film-search...\n')

    client = bigquery.Client()

    query_file = open(query_path, mode='r')

    query = query_file.read()

    print(f"""Running query in BQ:
          
          ---
          {query}
          ---\n
          """)

    query_results = run_query_in_bq(client, query)

    print('Searching boxofficemojo.com for films...\n')

    data = {}

    for ix, row in enumerate(query_results):
        title = row.title
        release_year = int(row.release_year)

        release_data = mojo_search_for_film_release(title,
                                                    release_year,
                                                    sleep_interval=sleep_interval)

        data[ix] = {
                'search_title': title,
                'search_release_year': release_year,
                'mojo_title': release_data['mojo_title'],
                'mojo_release_year': release_data['mojo_release_year'],
                'mojo_release_url': release_data['mojo_release_url']
                }
            
        print(f'\nData collected: {data[ix]}\n')
    
    df = pd.DataFrame.from_dict(data, orient='index')

    print('Writing dataframe to BQ:')

    return write_df_to_bq(client,
                          df,
                          schema_path=schema_path,
                          dataset_id='box_office_mojo',
                          table_name='mojo_film_search'
                          )

if __name__ == '__main__':

    status = mojo_film_search(
        query_path='query__mojo_film_search.sql',
        schema_path='schema__mojo_film_search.json',
        dataset_id='box_office_mojo',
        table_name='mojo_film_search',
        sleep_interval=3
        )
    
    print(status)