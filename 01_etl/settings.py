import os

from dotenv import load_dotenv

load_dotenv()

DATABASES = {
    'postgres': {
        'dbname': os.environ.get('DB_NAME_PSTGR'),
        'user': os.environ.get('DB_USER_PSTGR'),
        'password': os.environ.get('DB_PASSWORD_PSTGR'),
        'host': os.environ.get('DB_HOST_PSTGR'),
        'port': os.environ.get('DB_PORT_PSTGR'),
    },
    'elastic': {
        'host': os.environ.get('DB_HOST_ETL'),
        'port': os.environ.get('DB_PORT_ETL'),
    },
}

FILE_PATHS = {
    'state': os.environ.get('FILE_PATH_STATE'),
}
