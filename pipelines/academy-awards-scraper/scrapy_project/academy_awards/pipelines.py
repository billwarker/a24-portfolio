# Define pipelines to process your scraped items

# See documentation in:
# https://docs.scrapy.org/en/2.11/topics/item-pipeline.html

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from google.cloud import bigquery, storage
import logging
import re
import json
import os

class BigQueryPipeline:

    def __init__(self,
                 service_account_file,
                 bucket_name,
                 file_path,
                 file_name,
                 project_id,
                 dataset_id,
                 table_id):
        
        # Google BigQuery, Storage details
        self.storage_client = storage.Client()
        self.bigquery_client = bigquery.Client()

        self.service_account_file = service_account_file
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.file_name = file_name
        self.bigquery_table_id = f"{project_id}.{dataset_id}.{table_id}"

        self.payload = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            service_account_file=crawler.settings.get('BIGQUERY_SERVICE_ACCOUNT_FILE'),
            bucket_name=crawler.settings.get('GCS_BUCKET_NAME'),
            file_path=crawler.settings.get('PAYLOAD_FILE_PATH'),
            file_name=crawler.settings.get('PAYLOAD_FILE_NAME'),
            project_id=crawler.settings.get('BIGQUERY_PROJECT_ID'),
            dataset_id=crawler.settings.get('BIGQUERY_DATASET_ID'),
            table_id=crawler.settings.get('BIGQUERY_TABLE_ID')
            )
    
    def _process_best_original_song(self, item):
        
        if 'from ' in item['name']:
            name_text_parts = item['name'].split('; ')
            item['film'] = name_text_parts[0].replace('from ', '')
            item['name'] = name_text_parts[1]

        elif 'in "' in item['name']:
            name_part = item['name'].split('" ')[1]
            
            if name_part == '':
                name_part = None

            item['name'] = name_part

        return item
    
    def _process_best_international_feature_film(self, item):
        
        if item['year'] > 2017:
            item['film'] = item['name']
            item['name'] = None
        else:
            item['name'] = None

        return item
    
    def _remove_name_verbs(self, item):
        replacements = [
            'Written by ',
            'Story by ',
            'Screenplay by ',
            'Screenplay - ',
            'Screenplay by ',
            'Written for the screen by ',
            'Written for the Screen by ',
            'Art Direction: ',
            'Set Decoration: ',
            'Production Design: ',
            'Music and Lyric by ',
            'Music and Lyrics by ',
            'Music by ',
            'Lyrics by ',
            'Lyric by ',
            'Adaptation Score by ',
            'Song Score by ',
            'Orchestral Score by ',
            ', Producers',
            ', Producer',
            ', Sound Director'
            ]
        
        pattern = "|".join(replacements)
        
        item['name'] = re.sub(pattern, '', item['name'])

        return item
    
    def _process_multiple_names(self, item):

        name_text = item['name']

        name_text = name_text.replace(', Jr.', ' Jr.')
        name_text = name_text.replace(' and ', ',')
        name_text = name_text.replace(', & ', '')
        name_text = name_text.replace(' & ', ',')
        name_text = name_text.replace('; ', ',')
        name_text = name_text.replace(', ', ',')
        
        item['name'] = list(set(name_text.split(',')))



        return item
    
    def process_item(self, item, spider):

        if item['category'] == 'Music (Original Song)':
            item = self._process_best_original_song(item)

        if item['category'] in ('International Feature Film', 'Foreign Language Film'):
            item = self._process_best_international_feature_film(item)

        if item['name'] is not None:
            item = self._remove_name_verbs(item)
            item = self._process_multiple_names(item)

        self.payload.append(dict(item))

        return item
    
    def _upload_to_gcs(self, json_file):

        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f'{self.file_path}/{self.file_name}')

        blob.upload_from_filename(json_file)
        logging.info(f'{json_file} uploaded to {blob.public_url}')

    def _load_json_to_bq(self):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            schema=[
                bigquery.SchemaField("category", field_type='STRING'),
                bigquery.SchemaField("film", field_type='STRING'),
                bigquery.SchemaField('name', field_type='STRING', mode='REPEATED'),
                bigquery.SchemaField("type", field_type='STRING'),
                bigquery.SchemaField("year", field_type='INT64')
                ],
            write_disposition="WRITE_TRUNCATE"
            )
        
        load_job = self.bigquery_client.load_table_from_uri(
            f"gs://{self.bucket_name}/{self.file_path}/{self.file_name}",
            self.bigquery_table_id,
            job_config=job_config,
        )

        logging.info(load_job.result())

    def close_spider(self, spider):
        # Insert remaining items when the spider closes
        if self.payload:
            with open(self.file_name, 'w') as f:
                for record in self.payload:
                    json.dump(record, f)
                    f.write("\n")

            print(self.file_name)
            self._upload_to_gcs(self.file_name)
            self._load_json_to_bq()

            os.remove(self.file_name)
            logging.info(f'cleanup: removed file {self.file_name}')

