ARG PYTHON_VERSION=3.7

FROM python:${PYTHON_VERSION}-slim

COPY requirements.txt ./requirements.txt

RUN pip install --upgrade pip && \
	pip install --upgrade setuptools && \
	pip install -r ./requirements.txt && \
	rm -rf /root/.cache/pip/*

COPY . /app
WORKDIR /app

ENV LOG_LEVEL='INFO' \
	REDIS_HOST='redis_server' \
	PARSER_SOCKETIO_HOST='parser' \
	PYTHONUNBUFFERED=1

CMD python ./main.py
