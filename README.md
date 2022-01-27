# ti-dp

Threat Intelligence / Data Preprocessing

## Netflow Data 

### Preprocessing Pipeline

- Preprocessing streaming netflow data from Kafka, using Spark Streaming.
- The input of preprocessing pipeline are the raw netflow data.
- All IPs that will be anonymized must be configured in file ***ip-anonymize.txt***, using the following options:
    - A specific IP can be declared. ***i.e.*** 192.168.1.50
    - A range of IP addresses can be declared (comma separated). First address is the start of the range, and second address is the end of the specified range. **IMPORTANT NOTE:** Only IP addresses that belong in a subnet with mask /24 are supported in this format ***i.e.*** 192.168.1.50,192.168.1.75
    - A subnet can be delcared using the format *{IP_ADDRESS}/${MASK}*. ***i.e.*** 192.168.1.0/24
- Anonymization of addresses is implemented using CryptoPan algorithm. There will be a service that anonymizes IPs and stores them in a Redis database. Spark Streaming preprocessing app will send the IPs, that must be anonymized through HTTP POST requests. The response of the request will be the anonymized IP, which will take place of the original IP address in csv record. Service also provides the ability to de-anonymize IP addresses.
- Preprocessing pipeline has three outputs:
    - **anonymized** netflow data: Only anonymization function has been applied.
    - **preprocessed** netflow data: Only preprocessed functions have been applied.
    - **anonymized & preprocessed** netflow data: Both anonymization & preprocessing functions have been applied.
- Elasticsearch will store only data that are both preprocessed and anonymized. The rest two outputs will be available through Kafka (each output in a different topic).

### Anonymization & Preprocessing Fuctions

The following preprocessing functions have been implemented:

- Calculate number of total flow packets (tpkt).
- Calculate number of total flow bytes (tbytes). 
- Check if destination port is a port, where a common service runs (cp). As common services and its ports the following are considered: FTP(20,21), SSH(22), Telnet(23), SMTP(25), DNS(53), DHCP(67,68), TFTP(69), HTTP(80), POP3(110), NNTP(119), NTP(123), IMAP4(143), SNMP(161), LDAP(389), HTTPS(443), IMAPS(993), RADIUS(1812), AIM(5190)
- One-Hot Encoding in flow protocol (prtcp, prudp, pricmp, prigmp, prother).
- One-Hot Encoding in TCP flags (flga, flgs, flgf, flgr, flgp, flgu).

### Prerequisities

- Change schema column names (or order), if needed. *File*: netflow-raw-csv-schema.txt.
- Add any IP addresses (or subnets) that must be anonymized. *File*:ip-anonymize.txt

### Deployment
Spark Streaming Preprocessing App can be deployed in any Spark cluster, including k8s cluster.

#### Local deployment
- Change any needed configuration in .properties file of Netflow Preprocessing Pipeline (netflow-preprocessing.properties):
  - **kafka.broker.ip**: Hostname or IP address of Kafka instance, where raw netflow data are transmitted.
  - **kafka.broker.port**: Port of Kafka instance.
  - **kafka.topic.input**: Kafka topic, where preprocessing pipeline will read data from.
  - **kafka.topic.output**: Kafka topics, where output will be saved. In this configuration **3 topics** are needed (comma separated) with the following order: ${KAFKA_TOPIC_ANONYMIZED}, ${KAFKA_TOPIC_PREPROCESSED}, ${KAFKA_TOPIC_ANONYMIZED_AND_PREPROCESSED}.
  - **spark.app.name**: Name of Spark Streaming application.
  - **anonymization.endpoint**: Endpoint, where the IP addresses for anonymization will be sent. This must be in format http://${IP}:${PORT}/anonymizeRoute, ***i.e.*** http://127.0.0.1:8001/anonymize
  - **anonymization.columns**: Column names (comma separated) in data schema, that represents source, destination and other IP addresses that must be anonymized.
- Change any needed variables in src/main/scripts/start_netflow_preprocessing_app_no_k8s.sh. This is the script that submits the Spark app to the cluster. Any other spark-submit script or spark-submit CLI tool can be used. Most commons parameters that must be changed are the following ones:
  - **--master**: URL of the Spark master, where the app will run. ${SPARK_MASTER} value can set either directly in the script or as an environment variable.
  - **--deploy-mode**: Deploy mode of the Spark app. Client or cluster. In client mode the driver runs where this script is run and the executors are running in Spark cluster. In cluster mode both application driver and executors are running in Spark cluster.
  - **--conf spark.executor.instances**: How many Spark executors will run for this job.
- Any other needed --conf parameters can be found here: https://spark.apache.org/docs/latest/configuration.html
- ${SPARK_HOME} is defined as the folder where Spark binaries are installed. It can also be set as a variable or as an environmental parameter. 

#### Dockerized deployment
- Spark executor image must be built before deploying any application. For this a script is provided, named build_spark_exevutor_image.sh. This will build a Docker image named spark:v1 that will be used as executor image.
```
./src/main/scripts/start_netflow_preprocessing_app.sh
```
- If the tag of the image is different that v1, it must also be changed in file  in line ```--conf spark.kubernetes.container.image```.
- All required configuration are setup using Docker ENV params. The following ENV params can be used:
  - **K8S_MASTER**: K8s master, that will be used as Spark master. 
  - **K8S_SPARK_SRV_ACC**: Service account in k8s with required permissions for running Spark apps (Default: spark).
  - **DOCKER_IMAGE_REGISTRY**: Registry where image spark:v1 is saved. If it is saved in docker hub or locally, leave it empty. 
  - **SPK_DEPLOY_MODE**: Deploy mode of Spark App. Client/Cluster (Default: client). For now only client is tested.
  - **SPK_NUM_EXECUTORS**: Number of Spark executors to run (Default: 1).
  - **SPK_DRIVER_PORT**: Port of Spark driver, where executors will communicate with (Default: 40095).
  - **KAFKA_IP**: Hostname or IP address of Kafka instance, where raw netflow data are transmitted.
  - **KAFKA_PORT**: Port of Kafka instance.
  - **NETFLOW_INPUT_TOPICS**: Kafka topic, where preprocessing pipeline will read data from.
  - **NETFLOW_OUTPUT_TOPICS**: Kafka topics, where output will be saved. In this configuration **3 topics** are needed (comma separated) with the following order: ${KAFKA_TOPIC_ANONYMIZED},${KAFKA_TOPIC_PREPROCESSED},${KAFKA_TOPIC_ANONYMIZED_AND_PREPROCESSED}.
  - **SPARK_APP_NAME**: Name of Spark Streaming application.
  - **IP_ANON_ENDPOINT**: Endpoint, where the IP addresses for anonymization will be sent. This must be in format http://${IP}:${PORT}/anonymizeRoute, ***i.e.*** http://127.0.0.1:8001/anonymize
  - **IP_ANON_COLS**: Column names (comma separated) in data schema, that represents source, destination and other IP addresses that must be anonymized.
  - **BENCHMARK_MODE**: Run application in benchmark mode. Stats about anonymization process will be saved in a local file, named benchmark.txt.