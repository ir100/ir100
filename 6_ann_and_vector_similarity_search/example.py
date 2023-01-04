#!/usr/bin/env python3

from annoy import AnnoyIndex
from os import chdir
from os.path import dirname
from priority_queue import PriorityQueue

import pandas as pd
import tensorflow as tf
import tensorflow_hub as hub


if __name__ == '__main__':
    print('0.')
    embed = hub.load("https://tfhub.dev/google/universal-sentence-encoder/4")
    embeddings = embed([
        "The quick brown fox jumps over the lazy dog.",
        "I am a sentence for which I would like to get its embedding"])
    print(embeddings)

    print('1.')
    queries = ['Information Science', 'HDMI Cable']
    query_vectors = embed(queries)
    print(queries)
    print(query_vectors)

    print('2.')
    chdir(dirname(dirname(__file__)))
    parquet_path = './esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet'
    df_products = pd.read_parquet(parquet_path, columns=['product_locale', 'product_id', 'product_title'])
    df_products = df_products['us' == df_products.product_locale]
    df_products.set_index('product_id', drop=False, inplace=True)

    df_products = df_products.sample(frac=0.1, random_state=0)

    def batch_push(buffer_title, buffer_id, query_vectors, priority_queues):
        product_vectors = embed(buffer_title)
        for product_id, matmul in zip(buffer_id, tf.matmul(product_vectors, tf.transpose(query_vectors))):
            for priority_queue, dot_product in zip(priority_queues, matmul):
                priority_queue.push((dot_product.numpy(), product_id))
        del buffer_title[:]
        del buffer_id[:]

    priority_queues, buffer_id, buffer_title, batch_size = [], [], [], len(df_products) ** 0.5
    for _ in query_vectors:
        priority_queues.append(PriorityQueue(10))
    for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
        buffer_id.append(product_id)
        buffer_title.append(product_title)
        if batch_size < len(buffer_id):
            batch_push(buffer_title, buffer_id, query_vectors, priority_queues)
    if 0 < len(buffer_id):
        batch_push(buffer_title, buffer_id, query_vectors, priority_queues)
    exact_rankings = []  # For 7.
    for query, priority_queue in zip(queries, priority_queues):
        print(query)
        exact_rankings.append([])
        while 0 < len(priority_queue.body):
            priority, product_id = priority_queue.pop()
            print(priority, product_id, df_products.at[product_id, 'product_title'])
            exact_rankings[-1].append(product_id)

    print('3.')
    product_ids, annoy_index = [], AnnoyIndex(512, 'euclidean')

    def batch_add_item(buffer_title, buffer_id, annoy_index, product_ids):
        product_vectors = embed(buffer_title).numpy()  # Ref: https://github.com/spotify/annoy/issues/498
        for product_id, product_vector in zip(buffer_id, product_vectors):
            annoy_index.add_item(len(product_ids), product_vector)
            product_ids.append(product_id)
        del buffer_title[:]
        del buffer_id[:]

    for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
        buffer_id.append(product_id)
        buffer_title.append(product_title)
        if batch_size < len(buffer_id):
            batch_add_item(buffer_title, buffer_id, annoy_index, product_ids)
    if 0 < len(buffer_id):
        batch_add_item(buffer_title, buffer_id, annoy_index, product_ids)
    annoy_index.build(10)
    for query, query_vector in zip(queries, query_vectors):
        print(query)
        for i, distance in zip(*annoy_index.get_nns_by_vector(query_vector, 10, include_distances=True)):
            product_id = product_ids[i]
            print(distance, product_id, df_products.at[product_id, 'product_title'])

    # 4.
    df_centroids, annoy_index = df_products.sample(1000, random_state=0), AnnoyIndex(512, 'euclidean')
    for centroid_id, centroid_vector in enumerate(embed(df_centroids['product_title']).numpy()):
        annoy_index.add_item(centroid_id, centroid_vector)
    annoy_index.build(10)

    # 5.
    inverted_index_centroid_id = {}

    def batch_index(buffer_title, buffer_id, annoy_index, inverted_index_centroid_id):
        product_vectors = embed(buffer_title).numpy()
        for product_id, product_vector in zip(buffer_id, product_vectors):
            for centroid_id in annoy_index.get_nns_by_vector(product_vector, 1):
                if centroid_id in inverted_index_centroid_id:
                    inverted_index_centroid_id[centroid_id].append(product_id)
                else:
                    inverted_index_centroid_id[centroid_id] = [product_id]
        del buffer_title[:]
        del buffer_id[:]

    df_products.sort_index(inplace=True)
    for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
        buffer_id.append(product_id)
        buffer_title.append(product_title)
        if batch_size < len(buffer_id):
            batch_index(buffer_title, buffer_id, annoy_index, inverted_index_centroid_id)
    if 0 < len(buffer_id):
        batch_index(buffer_title, buffer_id, annoy_index, inverted_index_centroid_id)

    print('6.')
    approx_rankings = []  # For 7.
    for query, query_vector in zip(queries, query_vectors):
        print(query)
        priority_queue = PriorityQueue(10)
        approx_rankings.append([])
        for centroid_id in annoy_index.get_nns_by_vector(query_vector, 1):
            buffer_id += inverted_index_centroid_id[centroid_id]
            for product_id in buffer_id:
                buffer_title.append(df_products.at[product_id, 'product_title'])
            batch_push(buffer_title, buffer_id, [query_vector], [priority_queue])
        while 0 < len(priority_queue.body):
            priority, product_id = priority_queue.pop()
            print(priority, product_id, df_products.at[product_id, 'product_title'])
            approx_rankings[-1].append(product_id)

    print('7.')
    for query, exact_ranking, approx_ranking in zip(queries, exact_rankings, approx_rankings):
        print(query)
        print(len(set(exact_ranking) & set(approx_ranking)) / len(exact_ranking))
