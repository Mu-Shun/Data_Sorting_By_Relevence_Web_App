"""
-------------------------------------------------------------------------------------------------------------------

   Project: Web Application, supporting search by keywords.
   Databases:
       1. World:
                 consists of country, city and countrylanguage of World database
                 @https://dev.mysql.com/doc/world-setup/en/
       2. FilmsActors:
                consists of film, film_actor, language and actor of Sakila database
                @https://dev.mysql.com/doc/sakila/en/
       3. CustomersOrder:
                consists of products, orderdetails, productlines, orders and customers of classicmodels database
                @https://www.mysqltutorial.org/mysql-sample-database.aspx


    Execution Format:
    $ python import.py <mysql-database-name> <firebase-database-nodename>

    e.g.
    $ python import.py World World
    $ python import.py FilmsActors FilmsActors
    $ python import.py CustomersOrder CustomersOrder

-------------------------------------------------------------------------------------------------------------------
"""

import mysql.connector
from mysql.connector import errorcode
import json
import decimal
import datetime
import sys
import requests
import re


def _build_connector(project_metadata):
    """
    This function builds the connector instance to a database, with handling the exception.
    """
    try:
        # Creates connector to database
        cnx = mysql.connector.connect(host='localhost', user='inf551', password='inf551',
                                      database=project_metadata['database'])
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Wrong username or password!")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist!")
        else:
            print(err)
    return cnx


def _read_database(db_name, project_metadata):
    """
    This function transforms the database in MySQL server to JSON serializable Object.
    """
    cnx = _build_connector(project_metadata)

    result = _query_data(cnx, project_metadata)

    cnx.close()
    return result


def _query_data(cnx, project_metadata):
    """
    This function queries the data in the database, and transforms the data into JSON.

    e.g.
    project_metadata = {'database': 'World', 'tables': ['city', 'country', 'countrylanguage']}

                ===>

    {'city': {'1': {'CountryCode': 'AFG',
                    'District': 'Kabol',
                    'ID': 1,
                    'Name': 'Kabul',
                    'Population': 1780000},
                ...
     'country': {'ABW': {'Capital': 129,
                         'Code': 'ABW',
                         'Code2': 'AW',
                         'Continent': 'North America',
                         'GNP': '828.00',
                         ...
                         'SurfaceArea': '193.00'},
                ...
                }

     'countrylanguage': {'ABWDutch': {'CountryCode': 'ABW',
                                      'IsOfficial': 'T',
                                      'Language': 'Dutch',
                                      'Percentage': '5.3'},
                         ...
                         }
    }
    """

    data = dict()
    for table in project_metadata['tables']:  # iterate tables chosen.
        data[table] = dict()
        #  https://stackoverflow.com/questions/29772337/python-mysql-connector-unread-result-found-when-using-fetchone
        cursor = cnx.cursor(buffered=True)

        """
        1. Query the primary keys
        """
        primaryKey = _retrieve_primary_key(table, project_metadata)

        """
        2. Query the data
        """
        columns = _retrieve_columns(table, project_metadata)
        cursor.execute("SELECT {} FROM {}".format(', '.join(columns), table))
        table_values = cursor.fetchall()
        for row_values in table_values:
            tmp = dict()
            # Handle the values that are not JSON serializable.
            for attr_name, row_value in zip(columns, row_values):
                if not isinstance(row_value, (set, decimal.Decimal, datetime.date)):
                    tmp[attr_name] = row_value
                elif isinstance(row_value, set):
                    tmp[attr_name] = list(row_value)
                elif isinstance(row_value, (datetime.date, decimal.Decimal)):
                    tmp[attr_name] = str(row_value)
            attr_pk = _normalize_primaryKey('&'.join([str(tmp[pk]) for pk in primaryKey]))  # normalize key
            data[table][attr_pk] = tmp
        cursor.close()

    return data


def _retrieve_primary_key(table, project_metadata):
    """
    This function returns a list of column(s), which is the primary key(s) of <table>.
    """
    metadata_cnx = _build_connector({'database': 'INFORMATION_SCHEMA'})
    cursor = metadata_cnx.cursor(buffered=True)
    query = "SELECT kcu.COLUMN_NAME " \
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu " \
            "USING(CONSTRAINT_NAME,TABLE_SCHEMA,TABLE_NAME) " \
            "WHERE tc.TABLE_SCHEMA='{}' AND tc.TABLE_NAME='{}' " \
            "AND tc.CONSTRAINT_TYPE='PRIMARY KEY';".format(project_metadata['database'], table)
    cursor.execute(query)
    metadata_cnx.close()
    return [i[0] for i in cursor.fetchall()]


def _retrieve_columns(table, project_metadata):
    """
    This function returns column(s) of <table>.
    """
    metadata_cnx = _build_connector({'database': 'INFORMATION_SCHEMA'})
    cursor = metadata_cnx.cursor(buffered=True)
    query = "SELECT COLUMN_NAME FROM COLUMNS WHERE TABLE_SCHEMA=\'{}\' AND TABLE_NAME=\'{}\'"\
        .format(project_metadata['database'], table)
    cursor.execute(query)
    metadata_cnx.close()
    return [i[0] for i in cursor.fetchall()]


def _normalize_primaryKey(string):
    """
    Normalizes the string.

    e.g.
    'North America' -> ['north', america']
    'Washington.DC' -> ['woshingtondc']
    ...
    """
    return re.sub(r'[^A-Za-z0-9 ]+', '', string)  # only keeps english letter


def _normalize_index(value):
    """
    Normalize the string and split string if needed.
    e.g.
    'North America' -> ['north', america']
    'Washington.DC' -> ['woshingtondc']
    ...
    """
    # lower the value, and only keep letters, numbers and spaces in the value.

    string = str(value)
    _ = re.sub(r'[^A-Za-z0-9 ]+', '', string.lower()).strip()  # only keeps english letter
    # _ = re.sub(r'[^\w\s]+', '', string.lower()).strip()

    # split the value by space(s)
    _ = re.split(r'\s+', _)
    return _


def _patch_data_to_firebase(data, node):
    """
    Patches data to firebase.
    """
    URL = 'https://inf551-project-msl-wqd.firebaseio.com/{}.json'.format(node)
    print('Patching data to {}...'.format(URL))
    requests.patch(URL, json.dumps(data))


def _save_json(json_content, json_path):
    """
    Save <json_content> to local.
    """
    with open(json_path, 'w') as file:
        file.write(json.dumps(json_content, indent=4))


def _project_metadata(db_name):
    """
    Typically, it is solid to use information_schema.tables to query the tables for each database.
    
    However, as we planned in the project proposal, only a subset of tables is chosen for each database,
    so we have to give EXPLICIT table names.
    """
    if db_name not in {'World', 'FilmsActors', 'CustomersOrder'}:
        print("The {} database is not available. Please choose database from 'World', 'FilmsActors' "
              "or 'CustomersOrder'".format(db_name))
        exit(0)

    if db_name == 'World':
        return {'database': 'World',
                'tables': ['city', 'country', 'countrylanguage']}
    elif db_name == 'FilmsActors':
        return {'database': 'sakila',
                'tables': ['film', 'film_actor', 'language', 'actor']}
    else:
        return {'database': 'classicmodels',
                'tables': ['products', 'orderdetails', 'productlines', 'orders', 'customers']}


def _build_index(data):
    """
    This function builds inverted index for every word inside data,
        storing mapping from word to its location in table.


    e.g. the index of "davidson" in the CustomersOrder database
    "davidson":
    [
        {
            "TABLE": "products",
            "PRIMARYKEY": "S101678",
            "COLUMN": "productName"
        },

            ...

        {
            "TABLE": "productlines",
            "PRIMARYKEY": "Motorcycles",
            "COLUMN": "textDescription"
        }
    ]
    """
    index = dict()
    for table_name, table_data in data.items():
        for primary_key, row_data in table_data.items():
            for column_name, value in row_data.items():
                words_list = _normalize_index(value)
                for word in words_list:
                    try:
                        int(word)
                        continue
                    except ValueError:
                        index[word] = index.get(word, []) + \
                                            [{'TABLE': table_name, 'PRIMARYKEY': primary_key, 'COLUMN': column_name}]
    if '' in index:
        del index['']
    return index


def _retrieve_schema(project_metadata):
    """
    e.g.
    project_metadata = {'database': 'World', 'tables': ['city', 'country', 'countrylanguage']}

    result:
        {
            "city": {
                "columns": [
                    "CountryCode",
                    "District",
                    "ID",
                    "Name",
                    "Population"
                ],
                "foreign_key": {
                    "CountryCode": {
                        "referenced_table": "country",
                        "referenced_column": "Code"
                    }
                }
            },
            "country": {
                    ...
            },
            "countrylanguage": {
                    ...
            }
        }
    """
    schema = dict()
    for table in project_metadata['tables']:
        schema[table] = dict()
        schema[table]['columns'] = _retrieve_columns(table, project_metadata)
        schema[table]['foreign_key'] = _retrieve_foreign_key_constraints(project_metadata, table)
    return schema


def _retrieve_foreign_key_constraints(project_metadata, table):
    """
    This function connects to INFORMATION_SCHEMA of MySQL DBMS, returns foreign keys constraint.
    """
    metadata_cnx = _build_connector({'database': 'INFORMATION_SCHEMA'})
    cursor = metadata_cnx.cursor(buffered=True)
    query = "SELECT kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME " \
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu " \
            "USING(CONSTRAINT_NAME,TABLE_SCHEMA,TABLE_NAME) " \
            "WHERE tc.TABLE_SCHEMA='{}' AND tc.TABLE_NAME='{}' " \
            "AND tc.CONSTRAINT_TYPE='FOREIGN KEY'".format(project_metadata['database'], table)

    fk_cons = dict()
    cursor.execute(query)
    for cons in cursor.fetchall():
        if cons[1] in project_metadata['tables']:
            fk_cons[cons[0]] = {'referenced_table': cons[1], 'referenced_column': cons[2]}
    metadata_cnx.close()

    return fk_cons


if __name__ == '__main__':
    database_name = sys.argv[1]
    firebase_node = sys.argv[2]

    _project_metadata = _project_metadata(database_name)

    print('\nRetrieving schema...')
    _schema = _retrieve_schema(_project_metadata)
    # _save_json(_schema, '{}_schema.json'.format(database_name))
    _patch_data_to_firebase(_schema, '{}_schema'.format(firebase_node))

    print('\nRetrieving data...')
    results = _read_database(database_name, _project_metadata)
    # _save_json(results, '{}.json'.format(database_name))
    _patch_data_to_firebase(results, database_name)

    print('\nBuilding indices...')
    inverted_index = _build_index(results)
    # _save_json(inverted_index, '{}_index.json'.format(firebase_node))
    _patch_data_to_firebase(inverted_index, '{}_index'.format(firebase_node))

    print('\nDone!')

