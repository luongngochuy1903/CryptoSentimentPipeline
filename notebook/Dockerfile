FROM jupyter/all-spark-notebook:python-3.9.13

USER root 
RUN apt-get update && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN mkdir -p /usr/local/spark/jars && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /usr/local/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o /usr/local/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.3/iceberg-spark-runtime-3.3_2.12-1.4.3.jar \
      -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.3_2.12.jar
    curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar \
      -o /usr/local/spark/jars/iceberg-aws-bundle-1.5.0.jar