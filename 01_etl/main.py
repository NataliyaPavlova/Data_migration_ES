from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import logging
from PostgresLoader import PostgresLoader
from pprint import pprint
import psycopg2
from psycopg2.extras import DictCursor
from settings import DATABASES
from ETLIndex import Movies, create_index

logging.basicConfig(
    filename='data_migration_log.log',
    level=logging.DEBUG
)


def main():
    dsl = DATABASES['postgres']
    dsl_etl = DATABASES['elastic']
    ts = datetime.now() - timedelta(days=456, seconds=10)
    logging.info("starting....")
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn, \
            Elasticsearch([dsl_etl,], scheme="http", verify_certs=False) as es:
        pg_data_loader = PostgresLoader(pg_conn)
        if not es.indices.exists(index="movies"):
            create_index(es)

        for row in pg_data_loader.load_changed_data(ts.date().isoformat()):
            # pprint(row)
            fields = Movies.__annotations__.keys()
            dict_row = {key: val for (key, val) in zip(fields, row)}
            # pprint(dict_row)
            etl_row = Movies(**dict_row)
            # pprint(etl_row)
            resp = es.index(index="movies", id=etl_row.id, document=etl_row.dict())
            print(resp['result'])
            exit()
            # transformed_row = transform(row)
            # load(transformed_row)


if __name__ == '__main__':
    main()
