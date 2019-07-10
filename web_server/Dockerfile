ARG PYTHON_VERSION=3.7

FROM python:${PYTHON_VERSION}-alpine

COPY requirements.txt ./requirements.txt

RUN apk add --no-cache build-base && \
	pip install --upgrade pip && \
	pip install --upgrade setuptools && \
	pip install -r ./requirements.txt && \
	rm -rf /root/.cache/pip/* && \
  	rm -rf /var/cache/apk/*

COPY . /app
WORKDIR /app

ENV REDIS_HOST='redis_server' \
	PARSER_SOCKETIO_HOST='parser' \
	PYTHONUNBUFFERED=1 \
	LOG_LEVEL='INFO' \
	PYTHONASYNCIODEBUG=1

CMD python ./server.py
