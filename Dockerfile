FROM python:3.13

RUN pip install poetry==2.1.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache \
    PATH="/app/.venv/bin:$PATH" 

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-root && \
    rm -rf $POETRY_CACHE_DIR

COPY src ./src
COPY README.md ./README.md

RUN poetry install

ENTRYPOINT ["poetry", "run"]