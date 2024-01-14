import functions_framework
import pandas as pd
import requests as re
from bs4 import BeautifulSoup

def scrape_page_section_table(t):
    
    page_section = t.find_previous('h2').text.removesuffix('[edit]')
    t_header = t.find_all('tr')[0]
    t_header_cols = [th.text.strip().removesuffix('[a]') for th in t_header.find_all('th')]
    
    data = {}
    
    for row_ix, row in enumerate(t.find_all('tr')[1:]):
        data[row_ix] = {}
        
        for th in row.find_all('th'):
            th.name = 'td'
                        
        for col_ix, col in enumerate(row.find_all('td')):

            data[row_ix][t_header_cols[col_ix]] = col.text.strip()

    return (page_section, data)
        
@functions_framework.http
def wiki_film_scraper(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    wiki_films_url = 'https://en.wikipedia.org/wiki/List_of_A24_films'

    page = re.get(url=wiki_films_url)
    soup = BeautifulSoup(page.text, 'html.parser')

    for h in soup.find_all('h3'):
        h.name = 'h2'

    tables = soup.find_all('table')

    table_data = []

    for t in tables:
        payload = scrape_page_section_table(t)            
        table_data.append(payload)
    
    valid_sections = ['2010s', '2020s', 'Dated films']
    final_df = pd.concat([pd.DataFrame.from_dict(t[1], orient='index') for t in table_data if t[0] in valid_sections], ignore_index=True)

    return final_df.to_string()
