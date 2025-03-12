FROM apache/airflow:2.10.3-python3.11

# Set Poetry home so it installs globally (not in root's home dir)
ENV POETRY_HOME=/opt/poetry
ENV PATH="$POETRY_HOME/bin:$PATH"

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y curl git build-essential libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Now switch to the airflow user
USER airflow
WORKDIR /opt/airflow

# Copy and install project dependencies
COPY pyproject.toml poetry.lock* ./

# Copy the entire project
COPY . /opt/airflow/data_engine
ENV PYTHONPATH=/opt/airflow/data_engine

EXPOSE 8080
