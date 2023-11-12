FROM python:3.10

# set lang
ENV LANG C.UTF-8

# Set the working directory to /app
WORKDIR /app

RUN \
    apt-get update\
    && apt install python3-pip -y

RUN apt-get install -y default-jre

# install
RUN pip install --upgrade pip
RUN pip install poetry

COPY poetry.lock /app
COPY pyproject.toml /app

RUN poetry install --without dev

COPY . /app

RUN \
    rm -r test \
    && rm .gitignore

CMD ["poetry", "run", "python", "main.py"]
