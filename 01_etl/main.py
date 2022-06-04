import logging
from datetime import datetime
from time import sleep

from ESLoader import ESLoader
from ESLoader import create_index
from PostgresLoader import PostgresLoader
from settings import PSSettings
from settings import ESSettings
from settings import StateSettings

from utils import JsonFileStorage
from utils import State
from utils import es_conn_context
from utils import pg_conn_context

logging.basicConfig(
    filename='data_migration.log',
    level=logging.DEBUG
)
log = logging.getLogger('main')


def main():
    ps_settings, es_settings, file_settings = PSSettings(), ESSettings(), StateSettings()
    dsl, dsl_etl = ps_settings.dict(), es_settings.dict()
    sleep_seconds = 10
    storage = JsonFileStorage(file_settings.state_file)
    s = State(storage)
    key = "last_modified_at"

    with es_conn_context(dsl_etl, log) as es:
        if not es.indices.exists(index="movies"):
            create_index(es)

    while True:
        state = s.get_state(key)
        if not state:
            state = datetime.min.date().isoformat()
            logging.info('File with the state is empty. Start working from {}'.format(state))

        logging.info('Start working from {} from state file'.format(state))

        with pg_conn_context(dsl, log) as pg_conn, es_conn_context(dsl_etl, log) as es:
            pg_data_loader = PostgresLoader(pg_conn)
            etl_loader = ESLoader(es)

            batch, ts = pg_data_loader.load_changed_data(state)
            etl_loader.load_to_elastic(batch)

            s.set_state(key, ts)

            logging.info('Sleeping for {} seconds'.format(sleep_seconds))
            sleep(sleep_seconds)


if __name__ == '__main__':
    main()
