from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import logging
from PostgresLoader import PostgresLoader
from pprint import pprint
import psycopg2
from psycopg2.extras import DictCursor
from settings import DATABASES
from ETLIndex import Movies, create_index, ETLLoader

logging.basicConfig(
    filename='data_migration_log.log',
    level=logging.DEBUG
)


def main():
    dsl = DATABASES['postgres']
    dsl_etl = DATABASES['elastic']
    ts = datetime.now() - timedelta(days=456, seconds=10)
    logging.info("Starting....")
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn, \
            Elasticsearch([dsl_etl,], scheme="http", verify_certs=False) as es:
        pg_data_loader = PostgresLoader(pg_conn)
        etl_loader = ETLLoader(es)

        if not es.indices.exists(index="movies"):
            create_index(es)

        batch = pg_data_loader.load_changed_data(ts.date().isoformat())
        etl_loader.load_to_elastic(batch)


if __name__ == '__main__':
    main()
