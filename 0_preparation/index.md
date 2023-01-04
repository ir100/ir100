# 0. 準備

## 0. 準備の準備

本ハンズオンで利用するデータセットの一部はGit LFSを介して提供されている。Git LFSをインストールせよ。

## 1. データセットの入手

本ハンズオンではAmazonのShopping Queries Datasetを利用する。以下から `git clone` せよ。

https://github.com/amazon-science/esci-data

## 2. ライブラリの入手

このデータセットの一部はParquet形式である。Parquet形式のファイルを読み込むためのライブラリを入手せよ。

## 3. 製品データの読み込み

ライブラリの機能で製品データ（以下）をメモリに読み込め。

```
./esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet
```

（想定実行時間：1分以内）

## 4. 製品データの確認

製品を1つランダムに取り出し表示せよ。何度か実行せよ。

## 5. クエリデータの読み込み

クエリデータ（以下）をメモリに読み込め。

```
./esci-data/shopping_queries_dataset/shopping_queries_dataset_examples.parquet
```

（想定実行時間：1分以内）

## 6. クエリデータの確認

クエリを1つランダムに取り出し表示せよ。何度か実行せよ。

## 7. フィルタリング

題材の製品データ、クエリデータそれぞれ `product_locale` が `us` であるものを抽出せよ。

これ以降、特に断りがなければ、以下の理由からこれらのサブセットを用いる。

- 単語がスペース区切りで扱いやすいため。
- `product_locale` 内では `product_id` がユニークで扱いやすいため。

また、それぞれ**英語製品データ**、**英語クエリデータ**と呼ぶ。

## 8. 結合

英語製品データと英語クエリデータを、`product_id` をキーとして結合せよ。

また、結合後のエントリを1つランダムに取り出し表示せよ。何度か実行せよ。
