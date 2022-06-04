from pydantic import BaseSettings
from pydantic import Field


class PSSettings(BaseSettings):
    dbname: str = Field(..., env="DB_NAME_PSTGR")
    user: str = Field(..., env="DB_USER_PSTGR")
    password: str = Field(..., env="DB_PASSWORD_PSTGR")
    host: str = Field(..., env="DB_HOST_PSTGR")
    port: int = Field(..., env="DB_PORT_PSTGR")

    class Config:
        env_file = '.env'


class ESSettings(BaseSettings):
    es_host: str = Field(..., env="DB_HOST_ETL")
    es_port: int = Field(..., env="DB_PORT_ETL")

    class Config:
        env_file = '.env'


class StateSettings(BaseSettings):
    state_file: str = Field(..., env="FILE_PATH_STATE")

    class Config:
        env_file = '.env'
