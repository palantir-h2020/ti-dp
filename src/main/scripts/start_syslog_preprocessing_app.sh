#!/bin/bash

/opt/spark/bin/spark-submit \
    --master k8s://${K8S_MASTER} \
    --deploy-mode ${SPK_DEPLOY_MODE} \
    --class syslog.SyslogPreprocessing \
    --conf spark.driver.host=$(hostname -I) \
    --conf spark.driver.port=${SPK_DRIVER_PORT} \
    --conf spark.executor.instances=${SPK_NUM_EXECUTORS} \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=${K8S_SPARK_SRV_ACC} \
    --conf spark.kubernetes.namespace=${K8S_NAMESPACE} \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.container.image=${DOCKER_IMAGE_REGISTRY}spark:v1 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    --files release/syslog/syslog-preprocessing.properties \
    release/PalantirPreprocessingApp-1.0-SNAPSHOT-jar-with-dependencies.jar release/syslog/syslog-preprocessing.properties