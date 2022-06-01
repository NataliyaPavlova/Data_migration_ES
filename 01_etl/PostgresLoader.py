import logging

import psycopg2
from psycopg2.extensions import connection as _connection


class PostgresLoader:

    def __init__(self, connection: _connection):
        self.conn = connection
        logging.info('initialization of Postgres Data Loader.....')

    def load_changed_data(self, ts: str, arraysize=500):

        query = "SELECT fw.id, " \
                        "fw.rating, " \
                        "COALESCE (ARRAY_AGG(DISTINCT g.name), '{}') genre, " \
                        "fw.title, " \
                        "fw.description, " \
                        "COALESCE (ARRAY_AGG(DISTINCT p.full_name) " \
                                   "FILTER(WHERE pfw.role = 'director'), " \
                                   "'{}') director, " \
                        "COALESCE (ARRAY_AGG(DISTINCT p.full_name) " \
                                   "FILTER(WHERE pfw.role = 'actor'), " \
                                   "'{}') actors_names, " \
                        "COALESCE (ARRAY_AGG(DISTINCT p.full_name) " \
                                   "FILTER(WHERE pfw.role = 'writer'), " \
                                   "'{}') writers_names, " \
                        "COALESCE (JSON_AGG(DISTINCT jsonb_build_object( " \
                                            "'id', p.id, 'name', p.full_name) " \
                                            ") FILTER(WHERE pfw.role = 'actor')," \
                                    "'[]') actors, " \
                        "COALESCE (JSON_AGG(DISTINCT jsonb_build_object( " \
                                           "'id', p.id, 'name', p.full_name) " \
                                            ") FILTER(WHERE pfw.role = 'writer'), " \
                                    "'[]') writer " \
                "FROM content.film_work fw " \
                "LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id " \
                "LEFT JOIN content.person p ON p.id = pfw.person_id " \
                "LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id " \
                "LEFT JOIN content.genre g ON g.id = gfw.genre_id " \
                "WHERE fw.updated_at > '%s' " \
                "GROUP BY fw.id " \
                "ORDER BY fw.updated_at" % ts

        try:
            cur = self.conn.cursor()
            cur.execute(query)
            while True:
                rows = cur.fetchmany(arraysize)
                if not rows:
                    break
                for row in rows:
                    yield row
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("Error: {0}".format(error))
            self.conn.rollback()

