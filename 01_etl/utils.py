import abc
import json
import logging
from contextlib import contextmanager
from functools import wraps
from time import sleep
from typing import Any
from typing import Optional

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor


@contextmanager
def pg_conn_context(dsl: dict):
    @backoff('postgres')
    def connect():
        return psycopg2.connect(**dsl, cursor_factory=DictCursor)

    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def es_conn_context(dsl: dict):
    @backoff('elasticsearch')
    def connect():
        es = Elasticsearch([dsl, ])
        es.info()
        return es

    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


def backoff(name, start_sleep_time=10, factor=2, border_sleep_time=20):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time, n = 0, 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    sleep_time = (
                        start_sleep_time * factor ** n
                        if sleep_time < border_sleep_time
                        else border_sleep_time
                    )
                    logging.info('Failed to connect to {}, sleeping for {} seconds'.format(name, sleep_time))
                    sleep(sleep_time)
                    n += 1

        return inner

    return func_wrapper


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class JsonFileStorage(BaseStorage):

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
            return {}

    def save_state(self, state: dict) -> None:
        states = self.retrieve_state()
        states.update(state)
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(states, f)
        except FileNotFoundError:
            pass


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        if value:
            self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        states = self.storage.retrieve_state()

        return states.get(key)
