FROM python:3.10-buster as builder

RUN pip install poetry==1.4.2

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /code

COPY pyproject.toml poetry.lock ./

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

# The runtime image, used to just run the code provided its virtual environment
FROM python:3.10-slim-buster as runtime

ENV VIRTUAL_ENV=/code/.venv \
    PATH="/code/.venv/bin:$PATH" \
    PYTHONPATH="/code:${PYTHONPATH}"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY liiatools /code/liiatools
COPY liiatools_pipeline /code/liiatools_pipeline

RUN apt-get update && apt-get install -y --no-install-recommends

# Clone the conf files into the docker container
EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
# CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "/code/liiatools_pipeline/repository.py"]
CMD ["dagster", "code-server", "start", "--host", "0.0.0.0", "--port", "4000",  "--python-file", "/code/liiatools_pipeline/repository.py"]