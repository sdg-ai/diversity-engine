FROM python:3.10 as requirements-stage

WORKDIR /tmp

RUN pip install poetry

COPY ./pyproject.toml ./poetry.lock* /tmp/

RUN poetry export --dev -f requirements.txt --output requirements.txt --without-hashes

#stage2
FROM python:3.10

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

RUN mkdir -p /diversity/models
COPY /src /diversity/src
COPY --from=requirements-stage /tmp/requirements.txt /diversity/requirements.txt

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /diversity/requirements.txt

WORKDIR /diversity/src
#ENV PYTHONPATH=${PYTHONPATH}:${PWD}:.:src

CMD  ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
