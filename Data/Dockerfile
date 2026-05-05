FROM python:3.12-slim

WORKDIR /app

COPY consumer.py .
COPY valid_consumer.py .
RUN pip install --no-cache-dir kafka-python psycopg2-binary

CMD ["python", "consumer.py"]
