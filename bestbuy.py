#!/usr/bin/python3
"""BestBuy API scraper for categories/stores/products data."""
from io import BytesIO
from zipfile import ZipFile
from time import time, sleep
from datetime import datetime
from contextlib import closing
import logging
import csv
import json
import os

import requests
from clint.textui import progress

API_KEY = ''
sources = ['categories', 'stores', 'products']
start_ts = datetime.fromtimestamp(time()).strftime('%Y-%m-%dT%H:%M:%S')
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)


def get_data(sources, timing):
    """Parsing BestBuy data from files."""
    path = 'Output' + '/' + timing
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError:
            logging.critical('Check filesystem permissions!')
            exit(1)
    for source in sources:
        print('Parsing {}... '.format(source))
        with BytesIO() as temp, closing(requests.get('''https://'''
                                                     '''api.bestbuy.com/'''
                                                     '''v1/{}.json.zip?'''
                                                     '''apiKey={}'''.
                                                     format(source, API_KEY),
                                                     stream=True)) as data:
            size = int(data.headers.get('content-length'))
            for chunk in progress.bar(data.iter_content(chunk_size=1024),
                                      expected_size=(size/1024) + 1):
                temp.write(chunk)
            with ZipFile(temp) as z:
                with open(path + '/' + source + '.csv', 'a') as csv_dump:
                    with z.open(z.namelist()[0]) as cols:
                        fieldnames = [i for i in json.loads(cols.read().
                                                            decode('utf-8')
                                                            )[0]]
                        writer = csv.DictWriter(csv_dump,
                                                fieldnames=fieldnames,
                                                extrasaction='ignore')
                        writer.writeheader()
                    for item in z.namelist():
                        with z.open(item) as json_data:
                            for i in\
                             json.loads(json_data.read().decode('utf-8')):
                                writer.writerow(i)
        sleep(1)


if __name__ == '__main__':
    get_data(sources, start_ts)
