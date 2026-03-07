FROM apache/airflow:2.7.0-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    postgresql-client \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install additional Airflow providers first with pinned versions
RUN pip install --no-cache-dir --upgrade pip setuptools && \
    pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.11.1 \
    apache-airflow-providers-redis==3.4.1 \
    apache-airflow-providers-celery==3.3.7 \
    apache-airflow-providers-slack==8.4.0

# Install Python dependencies from requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
