import logging
from datetime import datetime
from time import sleep

from ESLoader import ESLoader
from ESLoader import create_index
from PostgresLoader import PostgresLoader
from settings import DATABASES
from settings import FILE_PATHS
from utils import JsonFileStorage
from utils import State
from utils import es_conn_context
from utils import pg_conn_context

logging.basicConfig(
    filename='data_migration.log',
    level=logging.DEBUG
)


def main():
    dsl = DATABASES['postgres']
    dsl_etl = DATABASES['elastic']
    sleep_seconds = 10
    storage = JsonFileStorage(FILE_PATHS['state'])
    s = State(storage)
    key = "last_modified_at"

    while True:
        state = s.get_state(key)
        if not state:
            state = datetime.min.date().isoformat()
            logging.info('File with the state is empty. Start working from {}'.format(state))

        logging.info('Start working from {} from file'.format(state))

        with pg_conn_context(dsl) as pg_conn, es_conn_context(dsl_etl) as es:
            pg_data_loader = PostgresLoader(pg_conn)
            etl_loader = ESLoader(es)

            if not es.indices.exists(index="movies"):
                create_index(es)

            batch, ts = pg_data_loader.load_changed_data(state)
            etl_loader.load_to_elastic(batch)

            s.set_state(key, ts)

        logging.info('Sleeping for {} seconds'.format(sleep_seconds))
        sleep(sleep_seconds)


if __name__ == '__main__':
    main()
