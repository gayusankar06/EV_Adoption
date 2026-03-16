FROM apache/spark:3.5.1

USER root

WORKDIR /opt/project

# Upgrade pip first
RUN python3 -m pip install --upgrade pip

# Install required libraries with higher timeout
RUN pip install --default-timeout=1000 --no-cache-dir \
    requests \
    google-api-python-client

# pyspark already exists in the Spark image
# so we don't reinstall it

RUN mkdir -p \
    /opt/project/data_ingestion \
    /opt/project/data_lake \
    /opt/project/logs

CMD ["sleep", "infinity"]