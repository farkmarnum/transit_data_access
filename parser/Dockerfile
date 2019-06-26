ARG PYTHON_VERSION=3.7


FROM python:${PYTHON_VERSION} as python-build
WORKDIR /wheels
COPY requirements.txt /wheels/requirements.txt
RUN pip install --upgrade pip \
 && pip wheel -r /wheels/requirements.txt


FROM python:${PYTHON_VERSION}-slim
COPY --from=python-build /wheels /wheels
RUN pip install --upgrade pip && \
	pip install --upgrade setuptools && \
	pip install -r /wheels/requirements.txt -f /wheels && \
	rm -rf /wheels && \
	rm -rf /root/.cache/pip/*
COPY . /app
WORKDIR /app
ENV LOG_LEVEL='WARNING' \
	MTA_API_KEY='f775a76bd1960c98831b3c2b06c19bb5' \
	REDIS_HOST='redis_server' \
	PARSER_SOCKETIO_PORT=45654 \
	PYTHONUNBUFFERED=1

CMD python ./main.py
