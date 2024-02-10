# import functions_framework
# import pandas as pd
import requests as rq
import re
from bs4 import BeautifulSoup

from cloudevents.http import CloudEvent
from google.cloud import bigquery

def scrape_page_section_table(t):
    
    page_section = t.find_previous('h2').text.removesuffix('[edit]')
    t_header = t.find_all('tr')[0]
    t_header_cols = [th.text.strip().removesuffix('[a]') for th in t_header.find_all('th')]

    t_header_cols_fmt = []
    
    for col in t_header_cols:
        col = col.lower()
        col = col.replace(r' ', '_')
        col = re.sub("\(.*\)|\[.*\]|\.", "", col)
        t_header_cols_fmt.append(col)
    
    print(f"{page_section}: {t_header_cols_fmt}")

    data = {}
    
    for row_ix, row in enumerate(t.find_all('tr')[1:]):
        data[row_ix] = {}
        
        for th in row.find_all('th'):
            th.name = 'td'
                        
        for col_ix, col in enumerate(row.find_all('td')):

            data[row_ix][t_header_cols_fmt[col_ix]] = col.text.strip()

    return (page_section, data)

def define_wiki_df_schema(wiki_df):
    schema = []

    for col, dtype in zip(wiki_df.columns, wiki_df.dtypes):

        if col == 'release_date':
            bq_dtype = bigquery.enums.SqlTypeNames.DATE
        else:
            bq_dtype = bigquery.enums.SqlTypeNames.STRING
        
        bq_col = bigquery.SchemaField(col, bq_dtype)
        
        schema.append(bq_col)

    return schema

def write_df_to_bq(df, schema, dataset_id, table_name):
    client = bigquery.Client()

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

def wiki_film_scraper():

    wiki_films_url = 'https://en.wikipedia.org/wiki/List_of_A24_films'

    page = rq.get(url=wiki_films_url)
    soup = BeautifulSoup(page.text, 'html.parser')

    for h in soup.find_all('h3'):
        h.name = 'h2'

    tables = soup.find_all('table')

    table_data = []

    for t in tables:
        payload = scrape_page_section_table(t)            
        table_data.append(payload)
    
    valid_sections = ['2010s', '2020s']
    wiki_df = pd.concat([pd.DataFrame.from_dict(t[1], orient='index') for t in table_data if t[0] in valid_sections], ignore_index=True)
    
    print(wiki_df.info())

    # wiki_df = wiki_df.drop(labels=["ref"], axis=1)
    wiki_df.release_date = pd.to_datetime(wiki_df.release_date)

    wiki_df_schema = define_wiki_df_schema(wiki_df)

    return write_df_to_bq(wiki_df, wiki_df_schema, "wikipedia", "list_of_A24_films")

def subscribe(self, cloud_event: CloudEvent) -> str:

    status = wiki_film_scraper()

    return status

if __name__ == '__main__':
    print(bigquery)