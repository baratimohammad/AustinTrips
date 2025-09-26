FROM bitnami/spark:latest

WORKDIR /app
COPY ../etl /app/etl

RUN pip install pyspark pandas sqlalchemy psycopg2-binary

CMD ["bash"]
