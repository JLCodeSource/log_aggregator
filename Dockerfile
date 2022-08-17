# pull official base image
FROM python:3.10.4-slim-buster

# set working directory
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYHTONUNBUFFERED 1

# install system dependencies
RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get clean

# install python dependecies
RUN make install

# add app
COPY . . 