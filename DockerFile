FROM apache/airflow:2.10.3-python3.11

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

# Install specific version of Poetry as airflow user
RUN pip install poetry==1.8.4

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