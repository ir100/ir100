# 8. インデックスのライフサイクル

## 0. 準備

以下のエンドポイントを持つWebサーバを実装せよ。ただし、入出力のフォーマットはリクエストパラメータまたはJSONとせよ。JSONのスキーマは自由とする。

- `/update` : 製品群をHTTP POSTすると、`product_title` をスペースで区切り、転置インデックスを構築してメモリに保持する。
- `/select` : `query` をHTTP GETすると、それを単語と解釈して（つまり、複数の単語の入力に対応する必要はない）保持している転置インデックスを引き、TFを優先度として10件までの `product_id` を返す。

また、簡単に動作確認をせよ。動作確認のために、`product_title` も保持しておき、優先度および `product_id` と共に返せ。この動作のためには転置インデックスのほか、追加のデータ構造も必要になるが、以降これらをまとめて**セグメント**と呼ぶ。

## 1. 挿入

1回のHTTP POSTのサイズには限界があるので、複数回の `/update` に対応せよ。ただし、既存の転置インデックスを効率よく変更するのは難しいので、`/update` のたびに追加のセグメントを保持せよ。`/select` の際は、保持しているセグメント全てを走査せよ。

また、簡単に動作確認をせよ。

## 2. 論理削除

新たに以下のエンドポイントを実装せよ。

- `/delete` : `product_id`（複数可）をHTTP GETすると、それらが指す製品群を以降の `/select` の対象から外す。

ただし、既存の転置インデックスを効率よく変更するのは難しいので、`product_id` ごとの生存フラグをセグメントに含めよ。`/update` の際に立て、`/delete` の際に倒し、`/select` の際に参照せよ。

また、簡単に動作確認をせよ。

## 3. 更新

既存の製品と同じ `product_id` の製品が `/update` されたとき、既存の製品の生存フラグを倒せ。

また、簡単に動作確認をせよ。

## 4. マージと物理削除

セグメントが増えるとオーバヘッドが生じる。そこで、`/update` によってセグメントが11個になったら、製品数が少ない順に2個のセグメントを1個のセグメントにマージすることで、常に高々10個のセグメントを保持せよ。このとき、生存フラグが倒れている製品は削除せよ。

## 5. 更新ベンチマーク

英語製品データ全件を100以上に分割して `/update` しても、現実的な時間（目標：1分）で完了するようにせよ。さらに、その状態から再度 `/update`（更新）しても、現実的な時間（目標：2分）で完了するようにせよ。

> ヒント：マージの回数を抑えるために、必要以上に分割しないようにせよ。

## 6. 検索ベンチマーク

`product_title` 中の単語から1%をサンプリングせよ。それらの単語で順に `/select` して合計の所要時間を測定せよ。

## 7. 最適化？

新たに以下のエンドポイントを実装せよ。

- `/optimize` : HTTP GETすると、セグメントが高々1個になるまでマージを繰り返す。

`/optimize` 後、`6.` を再度行い、所要時間を比較せよ。

## 8. 排他制御

一般にWebサーバはリクエストを並列処理する。この場合にも問題がないよう、セグメントのRead/Writeロックを行え。

## 9. コミット

新たに以下のエンドポイントを実装せよ。

- `/commit`

`/delete` および `/update` は、その後 `/commit` するまで `/select` の結果に影響しないようにせよ。