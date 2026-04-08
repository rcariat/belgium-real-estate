FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_pipeline.py .

CMD ["python", "etl_pipeline.py", "--db-url", "postgresql+psycopg2://postgres:postgres@postgres:5432/real_estate"]