FROM bitnami/spark:3.5.4
USER root
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && mkdir -p /opt/spark_jobs && chown -R 1001:1001 /opt/spark_jobs

USER 1001
RUN curl -L https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar \
    -o /opt/bitnami/spark/jars/postgresql-42.6.0.jar
RUN curl -L https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.66/bundle-2.20.66.jar \
    -o /opt/bitnami/spark/jars/aws-bundle-2.20.66.jar
RUN curl -L https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.66/url-connection-client-2.20.66.jar \
    -o /opt/bitnami/spark/jars/aws-url-client-2.20.66.jar
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar \
    -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12.jar
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar --output /opt/bitnami/spark/jars/iceberg-aws-bundle-1.5.0.jar