#!/usr/bin/env python3

from argparse import ArgumentParser
from hashlib import md5
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
from priority_queue import PriorityQueue
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs
from urllib.parse import urlparse

import json


class Segment:
    # 4.
    @classmethod
    def merge(cls, segments):
        def merge_inner(iterator_i, iterator_j):
            try:
                entry_i = next(iterator_i)
            except StopIteration:
                return list(iterator_j)
            try:
                entry_j = next(iterator_j)
            except StopIteration:
                return list(iterator_i)
            result = []
            while True:
                if entry_i < entry_j:
                    result.append(entry_i)
                    try:
                        entry_i = next(iterator_i)
                    except StopIteration:
                        try:
                            while True:
                                result.append(entry_j)
                                entry_j = next(iterator_j)
                        except StopIteration:
                            return result
                else:
                    result.append(entry_j)
                    try:
                        entry_j = next(iterator_j)
                    except StopIteration:
                        try:
                            while True:
                                result.append(entry_i)
                                entry_i = next(iterator_i)
                        except StopIteration:
                            return result

        segments.sort(key=lambda segment: -len(segment.liveness_id))
        new_segment, old_segment_i, old_segment_j = Segment([]), segments.pop(), segments.pop()

        for word in old_segment_i.inverted_index_title.keys():
            if word in old_segment_j.inverted_index_title:
                posting_list = merge_inner(old_segment_i.live_iterator(word), old_segment_j.live_iterator(word))
            else:
                posting_list = list(old_segment_i.live_iterator(word))
            if 0 < len(posting_list):
                new_segment.inverted_index_title[word] = posting_list
        for word in (old_segment_j.inverted_index_title.keys() - old_segment_i.inverted_index_title.keys()):
            posting_list = list(old_segment_j.live_iterator(word))
            if 0 < len(posting_list):
                new_segment.inverted_index_title[word] = posting_list

        for old_segment in (old_segment_i, old_segment_j):
            for product_id, info in old_segment.info_title.items():
                if product_id in old_segment.liveness_id:
                    new_segment.info_title[product_id] = info

        new_segment.liveness_id = old_segment_i.liveness_id | old_segment_j.liveness_id

        segments.append(new_segment)

    def __init__(self, products):
        self.inverted_index_title, self.info_title, self.liveness_id = {}, {}, set()
        for product in sorted(products, key=lambda product: product['product_id']):
            product_id, product_title = product['product_id'], product['product_title']
            # Cf. 2.0
            counter = {}
            for word in product_title.split():
                if word in counter:
                    counter[word] += 1
                else:
                    counter[word] = 1
            for word, count in counter.items():
                if word in self.inverted_index_title:
                    self.inverted_index_title[word].append((product_id, count))
                else:
                    self.inverted_index_title[word] = [(product_id, count)]
            self.info_title[product_id] = product_title
            self.liveness_id.add(product_id)

    def live_iterator(self, word):
        for entry in self.inverted_index_title.get(word, []):
            if entry[0] in self.liveness_id:
                yield entry


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass


class SearchEngine(BaseHTTPRequestHandler):
    @classmethod
    def run(cls, host, port):
        cls.segments = []
        ThreadedHTTPServer((host, port), cls).serve_forever()

    def do_POST(self):
        response, result, contents = 200, {}, self.rfile.read(int(self.headers['content-length'])).decode('utf-8')

        if self.path.startswith('/update'):
            segments, new_segment = SearchEngine.segments, Segment(json.loads(contents))
            for old_segment in segments:
                old_segment.liveness_id -= new_segment.liveness_id
            segments.append(new_segment)
            if 10 < len(segments):
                Segment.merge(segments)
            result['success'] = 'updated {0} products'.format(len(new_segment.liveness_id))

        else:
            response, result['error'] = 404, 'unknown POST endpoint'

        self.send(response, result)

    def do_GET(self):
        response, result, parameters = 200, {}, parse_qs(urlparse(self.path).query)

        if self.path.startswith('/delete'):
            product_ids = set(parameters['product_id'])
            for segment in SearchEngine.segments:
                segment.liveness_id -= product_ids
            result['success'] = 'deleted {0} products if exist'.format(len(product_ids))

        # Cf. 9.5
        elif self.path.startswith('/fetch'):
            result['success'] = []
            for product_id in parameters['product_id']:
                for segment in SearchEngine.segments:
                    if product_id in segment.liveness_id:
                        result['success'].append(segment.info_title[product_id])
                        break

        elif self.path.startswith('/optimize'):
            segments = SearchEngine.segments
            while 1 < len(segments):
                Segment.merge(segments)
            result['success'] = 'optimized'

        elif self.path.startswith('/queries'):
            queries = set()
            for segment in SearchEngine.segments:
                queries |= segment.inverted_index_title.keys()
            result['success'] = []
            for query in sorted(queries):
                query_hash = int(md5(query.encode('utf-8')).hexdigest(), 16)
                if 0 == query_hash % 100:
                    result['success'].append(query)

        elif self.path.startswith('/select'):
            segments, priority_queue, ranking = SearchEngine.segments, PriorityQueue(10), []
            assert 1 == len(parameters['query'])
            for segment_index, segment in enumerate(segments):
                for product_id, tf in segment.live_iterator(parameters['query'][0]):
                    priority_queue.push((tf, product_id, segment_index))
            while 0 < len(priority_queue.body):
                priority, product_id, segment_index = priority_queue.pop()
                ranking.append({
                    '_priority': priority,
                    'product_id': product_id,
                })
                # Cf. 9.5
                if 'omit_detail' not in parameters:
                    ranking[-1]['product_title'] = segments[segment_index].info_title[product_id]
            result['success'] = ranking

        elif self.path.startswith('/truncate'):
            SearchEngine.segments = []
            result['success'] = 'truncated'

        else:
            response, result['error'] = 404, 'unknown GET endpoint'

        self.send(response, result)

    def send(self, response, result):
        self.send_response(response)
        self.send_header('Content-Type', 'text/json')
        self.end_headers()
        self.wfile.write(json.dumps(result, indent=4).encode('utf-8'))


if __name__ == '__main__':
    argument_parser = ArgumentParser(description='runs a search engine')
    argument_parser.add_argument('--host', default='127.0.0.1', help='host name', metavar='str', type=str)
    argument_parser.add_argument('--port', default=8080, help='port number', metavar='int', type=int)
    arg_dict = argument_parser.parse_args()
    SearchEngine.run(arg_dict.host, arg_dict.port)
