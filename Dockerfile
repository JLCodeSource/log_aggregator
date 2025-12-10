# pull official base image
FROM python:3.10-slim-bookworm

# set working directory
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYHTONUNBUFFERED=1

# install system dependencies
RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends make curl \
    && apt-get clean

# add app
COPY . .

# install poetry
RUN curl -sSL https://install.python-poetry.org | python3 - \
    # install python dependecies
    && PATH="$HOME/.local/bin:$PATH" \
    && make install \
    # Run lint
    && make lint \
    # Run tests
    && make test_unit

