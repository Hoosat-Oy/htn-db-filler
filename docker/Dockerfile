FROM python:3.10-alpine

ARG REPO_DIR

EXPOSE 8000

ENV HTND_HOSTS_1=127.0.0.1:42420
ENV SQL_URI=postgresql://user:password@127.0.0.1:5432/database
ENV VERSION=1.0
ENV START_HASH=f51085433aebcbb687655213217c85bf48ab904cba00a32bbcb18ae6739b5cbb
ENV GENESIS_HASH=c3003a4836c39a7965fc92241a2a2e9504399fe2694ff25276f7ea47dde7a783
ENV BATCH_PROCESSING=false

RUN apk --no-cache add \
  git \
  gcc \
  libc-dev \
  build-base \
  linux-headers \
  libpq-dev \
  dumb-init

RUN pip install \
  pipenv

RUN addgroup -S -g 55746 api \
  && adduser -h /app -S -D -g '' -G api -u 55746 api

WORKDIR /app

USER api

COPY --chown=api:api . /app

RUN pipenv install --deploy

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD pipenv run python main.py
