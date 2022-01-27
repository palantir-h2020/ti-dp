FROM openjdk:8

RUN apt-get update -y
RUN apt-get install -y openjdk-17-jre-headless maven

RUN wget https://downloads.lightbend.com/scala/2.12.12/scala-2.12.12.deb
RUN dpkg -i scala-2.12.12.deb
RUN rm -f scala-2.12.12.deb

RUN wget https://dlcdn.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz \
    && tar -xzvf spark-3.1.2-bin-hadoop3.2.tgz \
    && rm spark-3.1.2-bin-hadoop3.2.tgz \
    && mv spark-3.1.2-bin-hadoop3.2 /opt/spark

WORKDIR /home/palantir

# Copy Spark Streaming Preprocessing App in /home/palantir/ directory
COPY src ./src
COPY pom.xml ./

# Copy start script in home directory
COPY src/main/scripts/start_netflow_preprocessing_app.sh ./

# Build from source
RUN mvn clean compile package process-resources

# Set permissions
RUN chmod -R 777 /home/palantir
RUN chmod +x ./start_netflow_preprocessing_app.sh

# Set some environmental parameters
ENV K8S_MASTER https://10.101.41.193:6443
ENV K8S_SPARK_SRV_ACC spark
ENV DOCKER_IMAGE_REGISTRY 10.101.10.244:5000/
ENV SPK_DEPLOY_MODE client
ENV SPK_NUM_EXECUTORS 1
ENV SPK_DRIVER_PORT 40095
ENV KAFKA_IP 10.101.41.255
ENV KAFKA_PORT 9092
ENV NETFLOW_INPUT_TOPIC netflow-raw
ENV NETFLOW_OUTPUT_TOPICS netflow-anonymized,netflow-preprocessed,netflow-anonymized-preprocessed
ENV SPARK_APP_NAME Palantir Preprocessing Netflow Application
ENV IP_ANON_ENDPOINT http://ip-anonymization-service.ti-sph:8100/anonymize
ENV IP_ANON_COLS sa,da
ENV BENCHMARK_MODE false

RUN useradd -ms /bin/bash spark
USER spark

CMD [ "/bin/bash", "start_netflow_preprocessing_app.sh" ]
