import os

from dotenv import load_dotenv

load_dotenv()

DATABASES = {
    'postgres': {
        'dbname': os.environ.get('DB_NAME_PSTGR'),
        'user': os.environ.get('DB_USER_PSTGR'),
        'password': os.environ.get('DB_PASSWORD_PSTGR'),
        'host': '172.19.0.2',
        'port': 5432,
        },
    'sqlite': {
        'dbname': os.environ.get('DB_NAME_SQLITE'),
    }

}
