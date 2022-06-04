import logging

from psycopg2.extensions import connection as _connection


class PostgresLoader:

    def __init__(self, connection: _connection):
        self.conn = connection
        logging.info('initialization of Postgres Data Loader.....')

    def load_changed_data(self, ts: str, arraysize=50):
        """ Get filmwork data modified after given ts"""
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
                "WHERE fw.id in (SELECT DISTINCT pfw.film_work_id " \
                "                FROM content.person p " \
                "                LEFT JOIN content.person_film_work pfw ON p.id = pfw.person_id " \
                "                WHERE p.updated_at > '%s' " \
                "                UNION " \
                "                SELECT DISTINCT gfw.film_work_id " \
                "                FROM content.genre g " \
                "                LEFT JOIN content.genre_film_work gfw ON g.id = gfw.genre_id " \
                "                WHERE g.updated_at > '%s') " \
                "OR fw.updated_at > '%s' " \
                "GROUP BY fw.id " \
                "ORDER BY fw.updated_at" % (ts, ts, ts)

        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchmany(arraysize)

        # retrieve last_modified_at to save the state
        last_modified_row_id = rows[-1]['id']
        query_for_state = "SELECT updated_at from content.film_work WHERE id = '%s'" % last_modified_row_id
        cur.execute(query_for_state)
        last_modified_at = cur.fetchone()[0].date().isoformat()

        logging.info('Extracted {} rows from Postgres'.format(arraysize))
        return rows, last_modified_at
