# Use a slim version of Python 3.11
FROM python:3.11-slim

# Environment Variables
ENV PYTHONUNBUFFERED=1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VERSION=1.8.2 \
    PATH="/opt/poetry/bin:$PATH"

# Install system dependencies and Poetry
RUN apt-get update && apt-get install -y \
    curl git build-essential libpq-dev && \
    curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry python3 - --version $POETRY_VERSION && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy only the necessary project files for dependency installation
COPY pyproject.toml poetry.lock* ./

# Install dependencies with Poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Copy the rest of the project files
COPY . .

# Set the PYTHONPATH to include the "data" directory
ENV PYTHONPATH="/app"

# Default command to run the application
CMD ["poetry", "run", "python", "-m", "data.main"]