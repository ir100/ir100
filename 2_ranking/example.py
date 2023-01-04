#!/usr/bin/env python3

from math import log
from os import chdir
from os.path import dirname
from priority_queue import PriorityQueue

import pandas as pd


# 2.
def boolean_or_iterator(list_i, list_j):
    i, j = 0, 0
    try:
        while True:
            if list_i[i] < list_j[j]:
                yield list_i[i][0], list_i[i][1], 0
                i += 1
            elif list_j[j] < list_i[i]:
                yield list_j[j][0], 0, list_j[j][1]
                j += 1
            else:
                yield list_i[i][0], list_i[i][1], list_j[j][1]
                i += 1
                j += 1
    except IndexError:
        if i < len(list_i):
            for product_id, tf_i in list_i[i:]:
                yield product_id, tf_i, 0
        if j < len(list_j):
            for product_id, tf_j in list_j[j:]:
                yield product_id, 0, tf_j


# 4.
def idf(N, n):
    return log((N - n + 0.5) / (n + 0.5))


# 6.
def bm25_weight(tf, K1, B, length, avg_length):
    return (tf * (K1 + 1)) / (tf + K1 * (1 - B + B * length / avg_length))


# 7.
def bm25f_weight(tf, boost, B, length, avg_length):
    return tf * boost / (1 - B + B * length / avg_length)


if __name__ == '__main__':
    chdir(dirname(dirname(__file__)))
    df_products = pd.read_parquet('./esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet')
    df_products = df_products['us' == df_products.product_locale]

    # 0.
    inverted_index_title = {}
    for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
        counter = {}
        for word in product_title.split():
            if word in counter:
                counter[word] += 1
            else:
                counter[word] = 1
        for word, count in counter.items():
            if word in inverted_index_title:
                inverted_index_title[word].append((product_id, count))
            else:
                inverted_index_title[word] = [(product_id, count)]

    # 1.
    priority_queue = PriorityQueue(10)

    print('3.')
    posting_list_hdmi, posting_list_cable = inverted_index_title['HDMI'], inverted_index_title['Cable']
    for product_id, tf_hdmi, tf_cable in boolean_or_iterator(posting_list_hdmi, posting_list_cable):
        priority_queue.push((tf_hdmi + tf_cable, product_id))
    df_products.set_index('product_id', drop=False, inplace=True)
    while 0 < len(priority_queue.body):
        priority, product_id = priority_queue.pop()
        print(priority, product_id, df_products.at[product_id, 'product_title'])

    print('4.')
    N, df_hdmi, df_cable = len(df_products), len(posting_list_hdmi), len(posting_list_cable)
    for product_id, tf_hdmi, tf_cable in boolean_or_iterator(posting_list_hdmi, posting_list_cable):
        priority_queue.push((tf_hdmi * idf(N, df_hdmi) + tf_cable * idf(N, df_cable), product_id))
    while 0 < len(priority_queue.body):
        priority, product_id = priority_queue.pop()
        print(priority, product_id, df_products.at[product_id, 'product_title'])

    # 5.
    info_title, sum_length_title = {}, 0
    for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
        length_title = len(product_title.split())
        info_title[product_id] = {'length': length_title}
        sum_length_title += length_title
    avg_length_title = sum_length_title / N

    print('6.')
    K1, B = 1.2, 0.75
    for product_id, tf_hdmi, tf_cable in boolean_or_iterator(posting_list_hdmi, posting_list_cable):
        length_title = info_title[product_id]['length']
        bm25 = 0
        bm25 += bm25_weight(tf_hdmi, K1, B, length_title, avg_length_title) * idf(N, df_hdmi)
        bm25 += bm25_weight(tf_cable, K1, B, length_title, avg_length_title) * idf(N, df_cable)
        priority_queue.push((bm25, product_id))
    while 0 < len(priority_queue.body):
        priority, product_id = priority_queue.pop()
        print(priority, product_id, df_products.at[product_id, 'product_title'])

    print('7.')
    df_basics = len(df_products['Amazon Basics' == df_products.product_brand])

    def answer7():
        for product_id, tf_hdmi, tf_cable in boolean_or_iterator(posting_list_hdmi, posting_list_cable):
            boost_title, length_title = 2.0, info_title[product_id]['length']
            weight_hdmi = bm25f_weight(tf_hdmi, boost_title, B, length_title, avg_length_title)
            weight_cable = bm25f_weight(tf_cable, boost_title, B, length_title, avg_length_title)
            tf_basics = 'Amazon Basics' == df_products.at[product_id, 'product_brand']
            boost_brand, length_brand, avg_length_brand = 1.0, 1, 1
            weight_basics = bm25f_weight(tf_basics, boost_brand, B, length_brand, avg_length_brand)
            bm25f = 0
            bm25f += weight_hdmi / (K1 + weight_hdmi) * idf(N, df_hdmi)
            bm25f += weight_cable / (K1 + weight_cable) * idf(N, df_cable)
            bm25f += weight_basics / (K1 + weight_basics) * idf(N, df_basics)
            priority_queue.push((bm25f, product_id))
        while 0 < len(priority_queue.body):
            priority, product_id = priority_queue.pop()
            print(priority, product_id, df_products.at[product_id, 'product_brand'], df_products.at[product_id, 'product_title'])

    answer7()

    print('8.')
    K1 = 3.5
    answer7()
