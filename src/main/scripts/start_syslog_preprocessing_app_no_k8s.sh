#!/bin/bash

/opt/spark/bin/spark-submit \
    --master k8s://${SPARK_MASTER} \
    --deploy-mode ${SPK_DEPLOY_MODE} \
    --class syslog.SyslogPreprocessing \
    --conf spark.executor.instances=1 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    --files release/syslog/syslog-preprocessing.properties \
    release/PalantirPreprocessingApp-1.0-SNAPSHOT-jar-with-dependencies.jar release/syslog/syslog-preprocessing.properties