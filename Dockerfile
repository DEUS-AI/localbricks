# Use the latest Ubuntu image as the base
FROM ubuntu:22.04

# Update the package list and install Python, pip, Git, wget, JDK, unzip etc
RUN apt-get update && \
    apt-get install -y python3.10 python3-pip git wget default-jdk curl inotify-tools unzip jq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Python 3.10 as the default python version
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1

#install poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Ensure Poetry binaries are in PATH
ENV PATH="/root/.local/bin:$PATH"

RUN poetry --version

# Download and unpack Apache Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install Databricks CLI
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Set build arguments
ARG DATABRICKS_HOST
ARG DATABRICKS_TOKEN

# Set up Databricks CLI profile configuration using build arguments
RUN echo "[DEFAULT]" > ~/.databrickscfg && \
    echo "host = ${DATABRICKS_HOST}" >> ~/.databrickscfg && \
    echo "token = ${DATABRICKS_TOKEN}" >> ~/.databrickscfg

# Set working directory
WORKDIR /opt/deus_dev_env/

COPY ./dist ./dist
COPY pyproject.toml poetry.lock ./

# Install Python dependencies using Poetry
RUN poetry config virtualenvs.create false && \
    poetry install && \
    poetry lock --no-update

# Copy entrypoint script into the container
COPY entrypoint.sh .

# Ensure the entrypoint script has execute permissions
RUN chmod +x ./entrypoint.sh

# Set the entrypoint script
ENTRYPOINT ["./entrypoint.sh"]

ENV PYTHONPATH=/opt/deus_dev_env
