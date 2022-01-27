#!/bin/bash

$SPARK_HOME/bin/spark-submit \
    --master k8s://${SPARK_MASTER} \
    --deploy-mode ${SPK_DEPLOY_MODE} \
    --class netflow.NetflowPreprocessing \
    --conf spark.executor.instances=1 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    --files release/netflow/netflow-preprocessing.properties,release/netflow/netflow-preprocessing-dev.properties,release/netflow/ip-anonymize.txt,release/netflow/netflow-raw-csv-schema.txt \
    release/PalantirPreprocessingApp-1.0-SNAPSHOT-jar-with-dependencies.jar release/netflow/netflow-preprocessing.properties