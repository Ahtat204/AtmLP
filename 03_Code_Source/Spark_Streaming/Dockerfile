FROM recommendationsystem-spark:latest
USER root
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
#RUN pip install pandas
#COPY ../Spark/jars /opt/spark/jars
ENV SPARK_CASSANDRA_CONNECTOR_VERSION=3.5.0
ENV SCALA_VERSION=2.12
ENV KAFKA_TOPIC=transactions
## you can override this via docker-compose 's environment array
ENV KAFKA_HOST="localhost:9092"
COPY 03_Code_Source/Spark_Streaming/stream.py .
WORKDIR /app
CMD ["spark-submit", "--master","local[2]", "stream.py"]