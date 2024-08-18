import pandas as pd
import requests as rq
import json

from datetime import datetime

import time

from google.cloud import bigquery

def bq_schema_to_dict(schema_file, recursive_mode=False):
    f = open(schema_file)

    schema = json.load(f)

    out = {}

    def parse_schema_col_json(dict_obj, json_obj, recursive):
        
        col_name = json_obj['name']
        
        dict_obj[col_name] = None
        
        if 'fields' in json_obj.keys() and recursive:
    
            dict_obj[col_name] = {}
            
            for col in json_obj['fields']:
                dict_obj[col_name] = parse_schema_col_json(dict_obj[col_name], col, recursive)
    
        return dict_obj
            
        
    for col in schema:
        out = parse_schema_col_json(out, col, recursive_mode)

    return out

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

def tmdb_film_search(sess, headers, search_term):

    search_url = "https://api.themoviedb.org/3/search/movie"
    search_req = rq.Request('GET', search_url).prepare()

    search_params = {'query': search_term}
    
    search_req.prepare_url(search_url, search_params)
    search_req.prepare_headers(headers)

    print(f"Sending search request for: {search_term}")
    search_response = sess.send(search_req)

    if search_response.status_code == 200:
        
        return search_response.json()['results']
        
    else:
        print(f'Status Code: {search_response.status_code}')

def tmdb_movie_details(sess, headers, id):

    movie_url = f"https://api.themoviedb.org/3/movie/{id}"
    movie_req = rq.Request('GET', movie_url).prepare()

    movie_req.prepare_headers(headers)

    movie_response = sess.send(movie_req)

    if movie_response.status_code == 200:
    
        return movie_response.json()

    else:
        print(f'Status Code: {movie_response.status_code}')

def find_a24_film_in_tmdb(sess, headers, film_data, throttle=3):

    tmdb_payload = None

    search_results = tmdb_film_search(sess, headers, film_data['a24_title'])

    if (n_results := len(search_results)) > 0:
        print(f'Found {n_results} search result(s)')

    else:
        print(f"No results found for {film_data['title']}")
        return tmdb_payload

    print('Checking search results...')
          
    for tmdb_film in search_results:
        
        print(f'{throttle}s throttle...')
        time.sleep(throttle)
        
        id, title, release_date = tmdb_film['id'], tmdb_film['title'], tmdb_film['release_date']
        title_match = film_data['a24_title'].lower() == title.lower()

        print(f"{title} - {release_date}: https://www.themoviedb.org/movie/{id}")
        
        tmdb_details = tmdb_movie_details(sess, headers, id)

        if 'a24' in tmdb_details['homepage']:
            print(f"Found A24 film page! {tmdb_details['homepage']}")
            tmdb_payload = tmdb_details
            break

        elif 'a24' in [company['name'].lower() for company in tmdb_details['production_companies']]:
            print('Found A24 production company!')
            tmdb_payload = tmdb_details
            break

        elif n_results == 1 and title_match is True:
            print('Exact title match on a single search result!')
            tmdb_payload = tmdb_details
            break
        
        try:
            tmdb_release_date = datetime.strptime(release_date, '%Y-%m-%d')
            year_delta = abs(film_data['a24_release_date'].year - tmdb_release_date.year)

            if title_match is True and year_delta in (0, 1):
                print('Match on title and release year!')
                tmdb_payload = tmdb_details
                break

        except ValueError:
            pass

        except AttributeError:
            pass
         
    return tmdb_payload

def run_tmdb_api(query_path, schema_path, dataset_id, table_name, throttle=3):

    headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjZDRiN2JkZjJhNWQwZjg3YTA3NTU4MjEyNzg0MjFiNSIsIm5iZiI6MTcyMjcwMDYwMS43MDI0NDksInN1YiI6IjY2YWU1MTFlMjU1NzA2NGVjNTBiNmE1ZCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.Q5iha1RWT5mgdUNjBrifsmgzxc0zcHqI0IXW5Jdy6i8"
    }

    print('Establishing BigQuery client connection...')
    bq_client = bigquery.Client()

    print('Creating Requests session...')
    sess = rq.Session()

    query_sql = open(query_path, mode='r').read()

    print(f'Running query:\n---\n{query_sql}\n---')

    rows = bq_client.query(query_sql)

    data_payload = []
    film_data_schema = bq_schema_to_dict(schema_path)

    for row in rows:
        a24_title = row.title
        a24_release_date = row.release_date

        film_data = film_data_schema.copy()
        film_data['a24_title'] = a24_title
        film_data['a24_release_date'] = a24_release_date 

        tmdb_data = find_a24_film_in_tmdb(sess, headers, film_data, throttle)

        if tmdb_data is not None:
            for key in tmdb_data.keys():
                film_data[key] = tmdb_data[key]
        
        print(film_data)
        data_payload.append(film_data)

        print(f'{throttle}s throttle...')
        time.sleep(throttle)


    print('Writing data to dataframe...')
    df = pd.DataFrame(data_payload)
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df = df.drop(['a24_release_date'], axis=1)

    print(f'Writing dataframe to {dataset_id}.{table_name}')


    result = write_df_to_bq(bq_client,
                            df,
                            schema_path=schema_path,
                            dataset_id=dataset_id,
                            table_name=table_name
                            )
    
    return result

if __name__ == '__main__':

    result = run_tmdb_api(query_path='query__tmdb_film_details.sql',
                          schema_path='schema__tmdb_film_details.json',
                          dataset_id='tmdb',
                          table_name='film_details',
                          throttle=0.5
                          )
    
    print(result)