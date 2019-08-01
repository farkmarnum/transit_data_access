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

ENV PYTHONUNBUFFERED=1

CMD python ./main.py
