"""
-------------------------------------------------------------------------------------------------------------------
        This script
        1. fetches the data from indices node on firebase
        2. merges the result and ranks the tuple
        3. PUT the results to firebase for future retrieval

        Execution Format:
        $ python search_build.py --keyword=keyword --database=database

        e.g.
        $ python search_build.py --keyword='north america' --database='World'
-------------------------------------------------------------------------------------------------------------------
"""

from functools import reduce
import re
import sys
import requests
from collections import Counter
import pprint
import argparse
import json


def _normalize_keyword(keyword):
    """
    Normalizes keyword.
    """
    # lower the value, and only keep letters, numbers and spaces in the value.
    _ = re.sub(r'[^A-Za-z0-9 ]+', '', keyword.lower()).strip()
    # _ = re.sub(r'[^\w\s]+', '', keyword.lower()).strip()

    # split the value by space(s)
    _ = re.split(r'\s+', _)
    return _


def _transform_result(occurrence):
    """
    e.g.
    {'tomsk': [{'COLUMN': 'Name', 'PRIMARYKEY': '3615', 'TABLE': 'city'},
               {'COLUMN': 'District', 'PRIMARYKEY': '3615', 'TABLE': 'city'},
               {'COLUMN': 'District', 'PRIMARYKEY': '3719', 'TABLE': 'city'}]}

    ->

    {'tomsk': [('city', '3615', 'Name'),
               ('city', '3615', 'District'),
               ('city', '3719', 'District')]}

    """
    return occurrence['TABLE'], occurrence['PRIMARYKEY'], occurrence['COLUMN']


def _intersec(x, y):
    """
    Used in reduce function.
    x, y are Set Object.
    """
    return x & y


def _concat(x, y):
    """

    :param x: array
    :param y: array
    :return:
    """
    return x + y


def _search(keyword, db_name):
    """
    This function searches the related tuples with respect to the keyword on the corresponding firebase node.

    :param keyword
    :param db_name
    """

    """
    'xxx yyy' -> ['xxx', 'yyy']
    """
    words_list = _normalize_keyword(keyword)
    result = dict()

    """
    For each word in ['xxx', 'yyy'], fetch JSON data.
    """
    print('\nSearching the tuples related to \'{}\' in firebase...'
          .format(keyword, db_name))
    for word in words_list:
        URL = 'https://inf551-project-msl-wqd.firebaseio.com/{}_index/{}.json'.format(db_name, word)
        result[word] = requests.get(URL).json()

    """
    For some word in ['xxx', 'yyy'], there might be no matching record.
    Delete those keys whose values are "None"
    If result becomes a empty dict, return None since all the words in the keyword don't have matching node.
    """
    to_be_deleted = []
    for _, value in result.items():
        if isinstance(value, type(None)):
            to_be_deleted.append(_)
    for i in to_be_deleted:
        del result[i]

    if not result:
        return None
    print('Ranking the related tuples in the order of decreasing relevance...')
    _rank(result, db_name)


def _rank(result, db_name):
    """
    This function ranks the searching result passed from _search().

    :param result
    """

    """
    1. Find the intersection of the searching results of different words coming from the same one keyword.
    2. Merge the searching results of different words, count the merged results and sort them.
    """
    for word, occurrence in result.items():
        result[word] = [occur for occur in map(_transform_result, occurrence)]
    # print(result)

    intersection = reduce(_intersec, [set(value) for _, value in result.items()])
    intersection = set([(occur[0], occur[1]) for occur in intersection])

    merged_result = Counter([(v[0], v[1]) for v in reduce(_concat, [value for _, value in result.items()])]).most_common()

    """
    Gets the tables related to the occurrences of the words of the keyword
    """
    tables_set = set(occur[0][0] for occur in merged_result)
    # pprint.pprint(related_tables)
    # print(merged_result)

    """
    Splits the merged result by tables
    """
    result_by_table = dict()
    for table in tables_set:
        result_by_table[table] = [occur[0] for occur in merged_result if occur[0][0] == table]

    """
    Puts the tuples, where the keyword appears as a whole, ahead of the others.
    """
    output = dict()
    for table, res in result_by_table.items():
        output[table] = [occur[1] for occur in res if occur in intersection] \
                       + [occur[1] for occur in res if occur not in intersection]

    # pprint.pprint(output)
    _put_output_to_firebase(output, db_name)


def _put_output_to_firebase(output, db_name):
    """
    This function puts the output of searching to the 'search_result' node
    """
    data_node = 'https://inf551-project-msl-wqd.firebaseio.com/{}/'.format(db_name)
    search_result = dict()

    for table, result in output.items():
        for pk in result:
            search_result[table] = search_result.get(table, []) + [requests.get(data_node + '{}/'.format(table) +
                                                                                '{}.json'.format(pk)).json()]
    # pprint.pprint(search_result)
    URL = 'https://inf551-project-msl-wqd.firebaseio.com/search_result.json'
    print('Saving the results into firebase...')
    print('Finished!')
    requests.put(URL, json.dumps(search_result))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", type=str, default=None)
    parser.add_argument("--database", type=str, default='World', choices=['World', 'FilmsActors', 'CustomersOrder'])

    args = parser.parse_args()
    _search(args.keyword, args.database)
