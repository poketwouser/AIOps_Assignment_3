# from course docker-spark-cluster Dockerfile
# openjdk:17-slim is gone from docker hub, using eclipse-temurin instead. ray added to same image
FROM eclipse-temurin:17-jre-jammy AS builder

# Add Dependencies for PySpark (python3-pandas etc. come from pip below instead)
RUN apt-get update && apt-get install -y curl wget procps net-tools ca-certificates python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_VERSION=3.3.4 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz


FROM builder AS apache-spark

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

EXPOSE 8080 7077 7000 4040 6379 8265

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

RUN python -m pip install --no-cache-dir pyspark==3.3.4 "ray[default,data]==2.59.0" pandas==2.3.3 pyarrow==25.0.1

# non-root user so output/ files aren't owned by root
ARG HOST_UID=1000
RUN useradd -m -u ${HOST_UID} a3 && chown -R a3:a3 /opt/spark
USER a3

COPY start-spark.sh /

CMD ["/bin/bash", "/start-spark.sh"]
