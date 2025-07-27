FROM bitnami/spark:3.5.4
USER root
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && mkdir -p /opt/spark_jobs && chown -R 1001:1001 /opt/spark_jobs

USER 1001
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12.jar
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar --output /opt/bitnami/spark/jars/iceberg-aws-bundle-1.5.0.jar