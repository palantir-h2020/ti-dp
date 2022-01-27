package netflow

import logger.Logger
import org.apache.spark.sql.SparkSession
import spark.NetflowPreprocessingFunctions
import netflow.utils.Utils

/**
 * NetflowPreprocessingMain class. Empty class.
 *
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */
class NetflowPreprocessingMain {

}

/**
 * NetflowPreprocessing object. Main logic of Spark Preprocessing App. When it is called, it
 * takes one parameter. The relative (or full) path of configuration file. If this param is not
 * given, it search for a conf file, named netflow-preprocessing.properties in the same folder of
 * the JAR file, where it runs from. It loads and parses configuration file, file with IP subnets
 * to be anonymized and schema of netflow csv records. It implements Spark Structured Straming. It
 * connects to a Kafka topic, saves the fetched Kafka records in a dataframe, applies any operations
 * needed in them and sends them back to Kafka. It create three different outputs and send them to
 * three different Kafka topics: one output for anonymized netflows, one for preprocessed netflows
 * and one for both anonymized and preprocessed netflows.
 *
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */
object NetflowPreprocessing {
  /**
   * Logger instance.
   */
  val logger = new Logger()

  /**
   * Main method of NetflowPreprocessing object. It loads all required configuration and info from
   * files, subscribes to appropriate Kafka topic to read raw netflow records and starts three queries
   * for creating the three desired outputs to be written in Kafka.
   *
   * @param args Array[String] Contains all runtime arguments. Only one is used from this method. The
   *             first one, which is the path of the configuration file to be loaded. If this is not
   *             given, by default app searches for a file, named netflow-preprocessing.properties in
   *             the same location with the JAR file.
   */
  def main(args: Array[String]): Unit = {
    var configFilename = "netflow-preprocessing.properties"
    if(args.length >= 1) {
      configFilename = args(0)
    }

    val ipAnonymizeFilename: String = "release/netflow/ip-anonymize.txt"
    val schemaFilename: String = "release/netflow/netflow-raw-csv-schema.txt"

    // Create Utils and PreprocessingFunctions objects
    val utils: Utils = new Utils(configFilename, ipAnonymizeFilename, schemaFilename)
    val preprocessingFunctions: NetflowPreprocessingFunctions = new NetflowPreprocessingFunctions()

    val appProps = utils.getAppProps()
    val ipList = utils.getIps()
    val colsToAnonymize = utils.getColsToAnonymize()

    logger.info("Running app in benchmark mode: " + appProps.getProperty("benchmark.enabled"))

    val kafkaTopics = appProps.getProperty("kafka.topic.output").split(",")

    // Creating Spark objects
    val spark = SparkSession.builder()
      .appName(appProps.getProperty("spark.app.name"))
      .config("spark.streaming.concurrentJobs", 3)
      .getOrCreate()

    import spark.implicits._

    // Read from Kafka Stream
    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        appProps.getProperty("kafka.broker.ip")+":"+appProps.getProperty("kafka.broker.port"))
      .option("failOnDataLoss", false)
      .option("subscribe", appProps.getProperty("kafka.topic.input"))
      .option("startingOffsets", "latest")
      .load()
    val inputdDf = inputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    // Preprocess and write back to Kafka topics
    val anonymizeQuery = inputDf
      .withColumn("value", preprocessingFunctions.anonymizeRecord(
        appProps.getProperty("anonymization.endpoint"), colsToAnonymize, ipList, appProps.getProperty("benchmark.enabled").toBoolean)('value))
      .writeStream
      .queryName("Anonymization Kafka Stream")
      .format("kafka")
      .option("kafka.bootstrap.servers",
          appProps.getProperty("kafka.broker.ip")+":"+appProps.getProperty("kafka.broker.port"))
      .option("topic", kafkaTopics(0))
      .option("checkpointLocation", "./checkpoint/anonymize-query")
      .start()

    val preprocessQuery = inputDf
      .withColumn("value", preprocessingFunctions.preprocessRecord()('value))
      .writeStream
      .queryName("Preprocessing Kafka Stream")
      .format("kafka")
      .option("kafka.bootstrap.servers",
          appProps.getProperty("kafka.broker.ip")+":"+appProps.getProperty("kafka.broker.port"))
      .option("topic", kafkaTopics(1))
      .option("checkpointLocation", "./checkpoint/preprocess-query")
      .start()

    val anonymizePreprocessQuery = inputDf
      .withColumn("value", preprocessingFunctions.anonymizeRecord(
        appProps.getProperty("anonymization.endpoint"), colsToAnonymize, ipList, appProps.getProperty("benchmark.enabled").toBoolean)('value))
      .withColumn("value", preprocessingFunctions.preprocessRecord()('value))
      .writeStream
      .queryName("Anonymization & Preprocessing Kafka Stream")
      .format("kafka")
      .option("kafka.bootstrap.servers",
          appProps.getProperty("kafka.broker.ip")+":"+appProps.getProperty("kafka.broker.port"))
      .option("topic", kafkaTopics(2))
      .option("checkpointLocation", "./checkpoint")
      .start()

    anonymizeQuery.awaitTermination()
    preprocessQuery.awaitTermination()
    anonymizePreprocessQuery.awaitTermination()

    spark.stop()
  }
}
