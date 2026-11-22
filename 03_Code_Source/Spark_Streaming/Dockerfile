FROM ahtat204/spark-processing-job:latest
USER root
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
COPY ../Spark/jars /opt/spark/jars
ENV SPARK_CASSANDRA_CONNECTOR_VERSION=3.5.0
ENV SCALA_VERSION=2.12
WORKDIR /app
COPY ../Spark/main.py .
CMD ["spark-submit", "--master","local[2]", "main.py"]