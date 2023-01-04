#!/usr/bin/env python3

from os import chdir
from os.path import dirname
from pickle import dump
from pickle import load
from tempfile import NamedTemporaryFile

import pandas as pd


# 5.
def boolean_and(list_i, list_j):
    result, i, j = [], 0, 0
    try:
        while True:
            if list_i[i] < list_j[j]:
                i += 1
            elif list_j[j] < list_i[i]:
                j += 1
            else:
                result.append(list_i[i])
                i += 1
                j += 1
    except IndexError:
        pass
    return result


# 6.
def boolean_or(list_i, list_j):
    result, i, j = [], 0, 0
    try:
        while True:
            if list_i[i] < list_j[j]:
                result.append(list_i[i])
                i += 1
            elif list_j[j] < list_i[i]:
                result.append(list_j[j])
                j += 1
            else:
                result.append(list_i[i])
                i += 1
                j += 1
    except IndexError:
        if i < len(list_i):
            result += list_i[i:]
        if j < len(list_j):
            result += list_j[j:]
    return result


# 7.
def boolean_and_not(list_i, list_j):
    result, i, j = [], 0, 0
    try:
        while True:
            if list_i[i] < list_j[j]:
                result.append(list_i[i])
                i += 1
            elif list_j[j] < list_i[i]:
                j += 1
            else:
                i += 1
                j += 1
    except IndexError:
        if i < len(list_i):
            result += list_i[i:]
    return result


if __name__ == '__main__':
    chdir(dirname(dirname(__file__)))
    parquet_path = './esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet'
    columns = ['product_locale', 'product_id', 'product_title', 'product_brand']
    df_products = pd.read_parquet(parquet_path, columns=columns)
    df_products = df_products['us' == df_products.product_locale]

    # 1.
    dictionary = {}
    for product_title in df_products['product_title']:
        for word in product_title.split():
            if word in dictionary:
                dictionary[word] += 1
            else:
                dictionary[word] = 1
    assert 912923 == len(dictionary)

    print('2.')
    df_products.sort_values('product_id', inplace=True)
    posting_list = []
    for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
        if 'Information' in product_title.split():
            posting_list.append(product_id)
    print(posting_list)
    assert 110 == len(posting_list)

    # 3.
    inverted_index_title = {}
    for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
        for word in set(product_title.split()):
            if word in inverted_index_title:
                inverted_index_title[word].append(product_id)
            else:
                inverted_index_title[word] = [product_id]
    assert len(dictionary) == len(inverted_index_title)
    assert posting_list == inverted_index_title['Information']

    # 4.
    with NamedTemporaryFile() as ntf:
        with open(ntf.name, 'wb') as f:
            dump(inverted_index_title, f)
        with open(ntf.name, 'rb') as f:
            inverted_index_title = load(f)

    print('5.')
    result5 = boolean_and(inverted_index_title['Information'], inverted_index_title['Science'])
    print(result5)
    assert 3 == len(result5)
    assert sorted(set(inverted_index_title['Information']) & set(inverted_index_title['Science'])) == result5  # Test

    print('6.')
    result6 = boolean_or(inverted_index_title['Information'], inverted_index_title['Retrieval'])
    print(result6)
    assert 129 == len(result6)
    assert sorted(set(inverted_index_title['Information']) | set(inverted_index_title['Retrieval'])) == result6  # Test

    print('7.')
    result7 = boolean_and_not(result6, result5)
    print(result7)
    assert 126 == len(result7)
    assert sorted(set(result6) - set(result5)) == result7  # Test

    # 8.
    inverted_index_brand = {}
    for product_id, product_brand in zip(df_products['product_id'], df_products['product_brand']):
        if product_brand in inverted_index_brand:
            inverted_index_brand[product_brand].append(product_id)
        else:
            inverted_index_brand[product_brand] = [product_id]

    # 9.
    result9 = boolean_and_not(inverted_index_title['Amazon'], inverted_index_brand['Amazon Basics'])
    assert 8681 == len(result9)
    assert sorted(set(inverted_index_title['Amazon']) - set(inverted_index_brand['Amazon Basics'])) == result9  # Test
