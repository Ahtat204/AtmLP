FROM ahtat204/spark-processing-job:latest
USER root
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
RUN pip install pandas grpcio numpy pyarrow
RUN pip install --upgrade protobuf
#COPY ../Spark/jars /opt/spark/jars
ENV SPARK_CASSANDRA_CONNECTOR_VERSION=3.5.0
ENV SCALA_VERSION=2.12
WORKDIR /app
COPY 03_Code_Source/Spark_Batch/batch.py .
COPY ../../04_Datasets/Historical_Data ./Historical_Data
ENV Historical_Data_Path=/app/Historical_Data
CMD ["spark-submit", "--master","local[2]", "batch.py"]