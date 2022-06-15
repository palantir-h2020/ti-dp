#!/bin/bash

kubectl delete -f syslog-pod.yaml

kubectl delete -f netflow-pod.yaml

sudo docker build -t spark-preprocessing-app:v1.1 -f Dockerfile .

sudo docker tag spark-preprocessing-app:v1.1 10.101.10.244:5000/spark-preprocessing-app:v1.1

sudo docker push 10.101.10.244:5000/spark-preprocessing-app:v1.1

kubectl create -f syslog-pod.yaml

kubectl create -f netflow-pod.yaml

# sudo docker run -it 10.101.10.244:5000/spark-syslog-preprocessing-app:v1.0 /bin/bash

# sudo docker run spark-syslog-preprocessing-app:v1.0
