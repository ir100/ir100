#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from os import chdir
from os.path import dirname
from requests import get
from requests import post
from time import time

import json
import pandas as pd


def queries():
    response = get('http://127.0.0.1:8080/queries')
    return json.loads(response.text)['success']


def truncate(params={}):
    get('http://127.0.0.1:8080/truncate', params)


def update(json_object):
    post('http://127.0.0.1:8080/update', json.dumps(json_object))


if __name__ == '__main__':
    chdir(dirname(dirname(__file__)))
    parquet_path = './esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet'
    df_products = pd.read_parquet(parquet_path, columns=['product_locale', 'product_id', 'product_title'])
    df_products = df_products['us' == df_products.product_locale]

    df_products = df_products.sample(frac=0.5, random_state=0)

    post_size = len(df_products) // 100

    print('2.')

    def answer8_5():
        for _ in range(2):
            buffer, start = [], time()
            for i, (product_id, product_title) in enumerate(zip(df_products['product_id'], df_products['product_title'])):
                buffer.append({'product_id': product_id, 'product_title': product_title})
                if post_size <= len(buffer):
                    update(buffer)
                    buffer = []
            if 0 < len(buffer):
                update(buffer)
            print('Elapsed Time: {0} (s)'.format(time() - start))

    truncate({'new_replicas': '1', 'new_shards': '2'})
    answer8_5()

    print('4.')

    def answer8_6(max_workers):
        def fn(query):
            get('http://127.0.0.1:8080/select', params={'query': query})

        start = time()
        with ThreadPoolExecutor(max_workers) as tpe:
            for query in queries():
                tpe.submit(fn, query)
        print('Elapsed Time: {0} (s)'.format(time() - start))

    answer8_6(1)

    print('5.')
    start = time()
    for query in queries():
        get('http://127.0.0.1:8080/two_stage_select', params={'query': query})
    print('Elapsed Time: {0} (s)'.format(time() - start))

    print('6.')
    truncate({'new_replicas': '2', 'new_shards': '2'})
    answer8_5()

    print('7')
    answer8_6(2)
