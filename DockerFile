# Base image bumped to match the apache-airflow 3.2.2 resolved in poetry.lock.
# poetry install (below) reinstalls Airflow from the lock on top of this base, so
# the base tag and the locked version must agree to avoid a version tug-of-war.
FROM apache/airflow:3.2.2-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch to airflow user for Poetry installation
USER airflow
WORKDIR /opt/airflow

# Poetry 2.x: the lock file is written in the 2.x format, which Poetry 1.8.x
# cannot read. Must match the Poetry major version used to generate poetry.lock.
RUN pip install poetry==2.2.1

# Debug: Check Poetry access as airflow user
RUN echo "Checking Poetry as airflow user..." && \
    which poetry && \
    poetry --version

# Copy dependency files
COPY --chown=airflow:root pyproject.toml poetry.lock* ./

# Install dependencies
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --only main --no-root

ENV PYTHONPATH=/opt/airflow/data_engine
EXPOSE 8080