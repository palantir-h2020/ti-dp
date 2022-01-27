#!/bin/bash

# Build Spark executor image, for Spark app to work in k8s cluster

# Download spark binaries
wget https://dlcdn.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz ./
# Extract them
tar -xzvf spark-3.1.2-bin-hadoop3.2.tgz
# Delete compressed file
rm spark-3.1.2-bin-hadoop3.2.tgz
# Move them to /opt/spark
mv spark-3.1.2-bin-hadoop3.2 /opt/spark

# Change directory to /opt/spark
cd /opt/spark/

# Build docker image from spark
./bin/docker-image-tool.sh -t v1 build
