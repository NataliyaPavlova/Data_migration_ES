FROM python:3.10-buster
# В версии slim не могу установить либу psycopg2

WORKDIR /app
COPY requirements.txt /app/requirements.txt

RUN python -m venv env && \
    env/bin/pip install --upgrade pip && \
    env/bin/pip install -r /app/requirements.txt &&\
    mkdir media && mkdir static

COPY . .

ENV PYTHONUNBUFFERED=1 \
    PORT=8000 \
    PATH='/app/env/bin:${PATH}'

EXPOSE 8000

CMD gunicorn -b 0.0.0.0:8000 --log-level=debug config.wsgi

