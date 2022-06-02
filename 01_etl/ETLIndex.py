from pydantic import BaseModel
import uuid
from typing import Union, List, Dict
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging


class Movies(BaseModel):
    id: uuid.UUID
    imdb_rating: Union[float, None]
    genre: List[str]
    title: str
    description: Union[str, None]
    director: List[str]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[Dict]
    writers: List[Dict]


body = {
  "settings": {
    "refresh_interval": "1s",
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_"
        },
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "english_possessive_stemmer": {
          "type": "stemmer",
          "language": "possessive_english"
        },
        "russian_stop": {
          "type":       "stop",
          "stopwords":  "_russian_"
        },
        "russian_stemmer": {
          "type": "stemmer",
          "language": "russian"
        }
      },
      "analyzer": {
        "ru_en": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "english_stop",
            "english_stemmer",
            "english_possessive_stemmer",
            "russian_stop",
            "russian_stemmer"
          ]
        }
      }
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id": {
        "type": "keyword"
      },
      "imdb_rating": {
        "type": "float"
      },
      "genre": {
        "type": "keyword"
      },
      "title": {
        "type": "text",
        "analyzer": "ru_en",
        "fields": {
          "raw": {
            "type":  "keyword"
          }
        }
      },
      "description": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "director": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "actors_names": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "writers_names": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "actors": {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "name": {
            "type": "text",
            "analyzer": "ru_en"
          }
        }
      },
      "writers": {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "name": {
            "type": "text",
            "analyzer": "ru_en"
          }
        }
      }
    }
  }
}


def create_index(es):
    es.indices.create(index="movies", body=body)


class ETLLoader:

    def __init__(self, es: Elasticsearch):
        self.fields = Movies.__annotations__.keys()
        self.es = es
        logging.info('Initialization of Elastic....')

    def load_to_elastic(self, batch: list):
        etl_batch = []
        for row in list(batch)[0]:
            dict_row = {key: val for (key, val) in zip(self.fields, row)}
            etl_row = Movies(**dict_row)
            etl_batch.append({'index': {'_id': etl_row.id}})
            etl_batch.append(etl_row.dict())
        resp = self.es.bulk(index="movies", body=etl_batch)
        self.es.search(body={"query": {"match_all": {}}}, index='movies')
        self.es.indices.get_mapping(index='movies')




