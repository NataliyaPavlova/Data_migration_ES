import dataclasses
import logging
import sqlite3


class SQLiteLoader:

    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.curs = self.conn.cursor()

    def download_batch(self, table: str, model: dataclasses, batch_size: int):
        """Download data from SQLite table to array of dataclasses"""

        try:
            fields = model.__annotations__.keys()

            # read batch data from table
            self.curs.execute("SELECT * FROM {0};".format(table))
            while True:
                data = self.curs.fetchmany(batch_size)
                if not data:
                    break
                # parse data to array of dataclasses
                array_dataclasses = []
                for row in data:
                    row_to_insert = [row[field] for field in fields]
                    array_dataclasses.append(model(*row_to_insert))
                yield array_dataclasses

        except (Exception, sqlite3.DatabaseError) as error:
            logging.error("Error: {0}".format(error))
