FROM ahtat204/spark-processing-job:latest
USER root
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
RUN pip install pandas
#COPY ../Spark/jars /opt/spark/jars
COPY *.jar /root/.ivy2/jars/
ENV SPARK_CASSANDRA_CONNECTOR_VERSION=3.5.0
ENV SCALA_VERSION=2.12
ENV KAFKA_TOPIC=transactions
## you can override this via docker-compose 's environment array
ENV KAFKA_HOST=localhost:9200
COPY 03_Code_Source/Spark_Streaming/stream.py .
WORKDIR /app
CMD ["spark-submit", "--master","local[2]", "stream.py"]