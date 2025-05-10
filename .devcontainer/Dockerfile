FROM debian:bullseye

# Configura entorno y variables
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8

# Instala herramientas del sistema + Java + Python + librerías necesarias
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3.10 \
    python3-pip \
    wget \
    curl \
    git \
    netcat \
    nano \
    gnupg \
    software-properties-common \
    apt-transport-https \
    ca-certificates \
    librdkafka-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# Instala Spark
ENV SPARK_VERSION=3.5.1
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Instala paquetes Python
COPY requirements.txt /tmp/
RUN python3 -m pip install --upgrade pip && pip install -r /tmp/requirements.txt