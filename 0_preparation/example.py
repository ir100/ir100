#!/usr/bin/env python3

from os import chdir
# from os import system
from os.path import dirname
from os.path import exists
import pandas as pd


# For dry-run
def system(cmd):
    print(cmd)
    return 0


if __name__ == '__main__':
    chdir(dirname(dirname(__file__)))

    print('0.')
    assert 0 == system('brew install git-lfs')
    assert 0 == system('git lfs install')

    if not exists('esci-data'):
        print('1.')
        system('git clone git@github.com:amazon-science/esci-data.git')

    print('2.')
    assert 0 == system('pip3 install pandas pyarrow fastparquet')

    # 3.
    df_products = pd.read_parquet('./esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet')

    print('4.')
    for _ in range(5):
        print(df_products.sample())

    # 5.
    df_examples = pd.read_parquet('./esci-data/shopping_queries_dataset/shopping_queries_dataset_examples.parquet')

    print('6.')
    for _ in range(5):
        print(df_examples.sample())

    # 7.
    df_products = df_products['us' == df_products.product_locale]
    df_examples = df_examples['us' == df_examples.product_locale]

    print('8.')
    df_merged = pd.merge(df_products, df_examples, on='product_id')
    for _ in range(5):
        print(df_merged.sample())
