FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY consumer.py .
RUN pip install --no-cache-dir kafka-python psycopg2-binary

# Run the consumer
CMD ["python", "consumer.py"]
