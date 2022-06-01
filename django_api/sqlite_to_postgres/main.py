"""Migrate data from SQLite db to Postgres db"""
import logging
import sqlite3
from contextlib import contextmanager

import psycopg2
from config import db
from loaders.PostgresLoader import PostgresLoader
from loaders.SQLiteLoader import SQLiteLoader
from psycopg2.extras import DictCursor
from tablesClasses import TABLES

logging.basicConfig(
    filename='data_migration_log.log',
    level=logging.DEBUG
)


@contextmanager
def conn_context(db_path: str):
    """Context manager for sqlite connection"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def main():
    """Main module for data migration"""
    batch_size = 500
    dsl = db.DATABASES['postgres']

    with conn_context(db.DATABASES['sqlite']['dbname']) as sqlite_conn, \
            psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        sqlite_loader = SQLiteLoader(sqlite_conn)
        postgres_loader = PostgresLoader(pg_conn)
        for table, model in TABLES.items():
            for batch_data in sqlite_loader.download_batch(table, model, batch_size):
                postgres_loader.upload_data(table, model, batch_data)


if __name__ == '__main__':
    main()
