FROM apache/airflow:2.10.3-python3.11

USER root

# Install system dependencies and Poetry
RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    libpq-dev && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Debug: Check Poetry installation
RUN echo "Checking Poetry installation..." && \
    ls -la /root/.local/bin/ && \
    /root/.local/bin/poetry --version

# Set Poetry path
ENV PATH="/root/.local/bin:$PATH"

USER airflow
WORKDIR /opt/airflow

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