# Use the official Spark image from Docker Hub
FROM spark:3.5.3-scala2.12-java17-ubuntu

# The base image defaults to a non-root user; switch to root to install deps.
USER root
RUN mkdir -p /var/lib/apt/lists/partial \
 && apt-get update \
 && apt-get install -y --no-install-recommends python3-pip wget \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

WORKDIR /app
