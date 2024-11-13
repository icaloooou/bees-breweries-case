# -*- coding: utf-8 -*-

import os
import io
import json
import logging
import requests
import pandas as pd

import sys
sys.path.insert(0,"/opt/airflow/")
from utils import config, functions


logger = logging.getLogger(__name__)


def bronze_layer(version):
    url = config.url_api
    try:
        response = requests.get(url)
        data = json.loads(response.text)
        prefix_s3 = f"{config.bronze_layer}/year={version.split('/')[0].split('-')[0]}/month={version.split('/')[1]}/day={version.split('/')[-1]}/breweries.json"
        functions.write_data(config.bucket, data, prefix_s3, 'JSON')
    except Exception as e:
        logger.error(f'Error: {e}')


def silver_layer(version):
    try:
        read_key = f"{config.bronze_layer}/year={version.split('/')[0].split('-')[0]}/month={version.split('/')[1]}/day={version.split('/')[-1]}/breweries.json"
        data = functions.read_data(config.bucket, read_key)
        df = pd.read_json(data)
        df = df.astype(str)

        for (city, state), subset in df.groupby(['city', 'state']):
            buffer = io.BytesIO()
            subset.to_parquet(buffer, index=False)
            buffer.seek(0)

            city = city.lower().replace(' ', '_')
            state = state.lower().replace(' ', '_')
            write_key = f"{config.silver_layer}/year={version.split('/')[0].split('-')[0]}/month={version.split('/')[1]}/day={version.split('/')[-1]}/state={state}/city={city}/breweries.parquet"
            functions.write_data(config.bucket, buffer, write_key, 'PARQUET')
    except Exception as e:
        logger.error(f'Error: {e}')


def gold_layer(version):
    try:
        read_key = f"{config.silver_layer}/year={version.split('/')[0].split('-')[0]}/month={version.split('/')[1]}/day={version.split('/')[-1]}/"
        df = functions.read_many_data(config.bucket, read_key)

        buffer_type = io.BytesIO()
        qty_type = df['brewery_type'].value_counts()
        df_type = qty_type.to_frame().reset_index().rename(columns={'count':'total_type'})
        df_type.to_parquet(buffer_type, index=False)
        buffer_type.seek(0)
        write_key_type = f"{config.gold_layer}/year={version.split('/')[0].split('-')[0]}/month={version.split('/')[1]}/day={version.split('/')[-1]}/qty_type.parquet"
        functions.write_data(config.bucket, buffer_type, write_key_type, 'PARQUET')

        buffer_location = io.BytesIO()
        qty_location = df[['state', 'city']].value_counts()
        df_location = qty_location.to_frame().reset_index().rename(columns={'count':'total_type'})
        df_location.to_parquet(buffer_location, index=False)
        buffer_location.seek(0)
        write_key_location = f"{config.gold_layer}/year={version.split('/')[0].split('-')[0]}/month={version.split('/')[1]}/day={version.split('/')[-1]}/qty_location.parquet"
        functions.write_data(config.bucket, buffer_location, write_key_location, 'PARQUET')
    except Exception as e:
        logger.error(f'Error: {e}')
