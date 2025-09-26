

#!/bin/bash

# Add Spark to PATH
export PATH=/opt/bitnami/spark/bin:$PATH

# 1. Download raw CSV
mkdir -p data/raw
if [ ! -f data/raw/trips.csv ]; then
    echo "Downloading CSV..."
    wget -O data/raw/trips.csv "https://data.austintexas.gov/api/views/tyfh-5r8s/rows.csv?fourfour=tyfh-5r8s&cacheBust=1744129742&date=20250921&accessType=DOWNLOAD"
else
    echo "CSV already exists, skipping download."
fi

# 2. Download second raw CSV
if [ ! -f data/raw/Kiosks.csv ]; then
    echo "Downloading extra CSV..."
    wget -O data/raw/Kiosks.csv "https://data.austintexas.gov/api/views/qd73-bsdg/rows.csv?fourfour=qd73-bsdg&cacheBust=1745520602&date=20250923&accessType=DOWNLOAD"
else
    echo "Extra CSV already exists, skipping download."
fi


# 3. Download PostgreSQL JDBC driver if missing
JAR_PATH="/opt/spark/jars/postgresql-42.6.0.jar"
if [ ! -f "$JAR_PATH" ]; then
    mkdir -p /opt/spark/jars
    wget -O "$JAR_PATH" https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
fi

# 4. Export SPARK_JARS so Spark can find the driver
export SPARK_JARS="$JAR_PATH"

# 5. Run Spark ETL
spark-submit --jars "$JAR_PATH" /app/etl/etl_main.py

