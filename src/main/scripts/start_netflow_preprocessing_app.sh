#!/bin/bash

# Change values in .properties files
sed -i "s/kafka.broker.ip=localhost/kafka.broker.ip=${KAFKA_IP}/g" release/netflow/netflow-preprocessing.properties
sed -i "s/kafka.broker.port=9092/kafka.broker.port=${KAFKA_PORT}/g" release/netflow/netflow-preprocessing.properties
sed -i "s/kafka.topic.input=netflow-raw/kafka.topic.input=${NETFLOW_INPUT_TOPIC}/g" release/netflow/netflow-preprocessing.properties
sed -i "s/kafka.topic.output=netflow-anonymized,netflow-preprocessed,netflow-anonymized-preprocessed/kafka.topic.output=${NETFLOW_OUTPUT_TOPICS}/g" release/netflow/netflow-preprocessing.properties
sed -i "s/spark.app.name=Palantir Preprocessing Netflow Application/spark.app.name=${SPARK_APP_NAME}/g" release/netflow/netflow-preprocessing.properties
sed -i "s/anonymization.endpoint=http:\/\/localhost:8100\/anonymize/anonymization.endpoint=${IP_ANON_ENDPOINT//\//\\/}/g" release/netflow/netflow-preprocessing.properties
sed -i "s/anonymization.columns=sa,da/anonymization.columns=${IP_ANON_COLS}/g" release/netflow/netflow-preprocessing.properties
sed -i "s/benchmark.enabled=false/benchmark.enabled=${BENCHMARK_MODE}/g" release/netflow/netflow-preprocessing.properties

/opt/spark/bin/spark-submit \
    --master k8s://${K8S_MASTER} \
    --deploy-mode ${SPK_DEPLOY_MODE} \
    --class netflow.NetflowPreprocessing \
    --conf spark.driver.host=$(hostname -I) \
    --conf spark.driver.port=${SPK_DRIVER_PORT} \
    --conf spark.executor.instances=${SPK_NUM_EXECUTORS} \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=${K8S_SPARK_SRV_ACC} \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.container.image=${DOCKER_IMAGE_REGISTRY}spark:v1 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    --files release/netflow/netflow-preprocessing.properties,release/netflow/netflow-preprocessing-dev.properties,release/netflow/ip-anonymize.txt,release/netflow/netflow-raw-csv-schema.txt \
    release/PalantirPreprocessingApp-1.0-SNAPSHOT-jar-with-dependencies.jar release/netflow/netflow-preprocessing.properties