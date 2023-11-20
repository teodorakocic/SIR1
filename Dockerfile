FROM openjdk:11.0.11-jre-slim-buster as builder

# Add Dependencies for PySpark and Sedona
RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy libgeos-dev rustc cargo cmake protobuf-compiler

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_VERSION=3.0.2 \
HADOOP_VERSION=3.2 \
SEDONA_VERSION=1.1.0-incubating \
SEDONA_SPARK_VERSION=3.0 \
GEOTOOLS_WRAPPER_VERSION=1.1.0-25.2 \
SPARK_HOME=/opt/spark \
PYTHONPATH=/opt/spark/python \
PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz

COPY /apps/requirements.txt /requirements.txt

# COPY /databricks_mosaic-0.3.11-py3-none-any.whl /databricks_mosaic-0.3.11-py3-none-any.whl

RUN pip3 install shapely==1.8.4
RUN pip3 install apache-sedona==1.1.0
# RUN pip3 install databricks-mosaic

FROM builder as apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master"

EXPOSE 8080 7077 7000

RUN curl https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-${SEDONA_SPARK_VERSION}_2.12/${SEDONA_VERSION}/sedona-spark-shaded-${SEDONA_SPARK_VERSION}_2.12-${SEDONA_VERSION}.jar -o $SPARK_HOME/jars/sedona-spark-shaded-${SEDONA_SPARK_VERSION}_2.12-${SEDONA_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${GEOTOOLS_WRAPPER_VERSION}/geotools-wrapper-${GEOTOOLS_WRAPPER_VERSION}.jar -o $SPARK_HOME/jars/geotools-wrapper-${GEOTOOLS_WRAPPER_VERSION}.jar

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY start-spark.sh /

CMD ["/bin/bash", "/start-spark.sh"]
