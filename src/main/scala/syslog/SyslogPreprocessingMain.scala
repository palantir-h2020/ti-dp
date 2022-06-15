package syslog

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vector

import customLogger.CustomLogger
import syslog.utils.Utils
import spark.SyslogPreprocessingFunctions

/**
 * SyslogPreprocessingMain class. Empty class.
 *
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */
class SyslogPreprocessingMain {}

object SyslogPreprocessing {

  /**
   * Main method of SyslogPreprocessing object. It loads all required configuration and info from
   * files, subscribes to appropriate Kafka topic to read raw Syslog records and starts three queries
   * for creating the three desired outputs to be written in Kafka.
   *
   * @param args Array[String] Contains all runtime arguments. Only one is used from this method. The
   *             first one, which is the path of the configuration file to be loaded. If this is not
   *             given, by default app searches for a file, named Syslog-preprocessing.properties in
   *             the same location with the JAR file.
   */
  def main(args: Array[String]): Unit = {

    var configFilename = "syslog-preprocessing.properties"
    if (args.length >= 1) {
      configFilename = args(0)
    }

    // Logging settings
    val logger = new CustomLogger()
    Logger.getLogger("org").setLevel(Level.OFF)

    // Create Utils and PreprocessingFunctions objects
    val utils: Utils = new Utils(configFilename)
    val preprocessingFunctions: SyslogPreprocessingFunctions = new SyslogPreprocessingFunctions()

    val appProps = utils.getAppProps()

    logger.info("Running app in benchmark mode: " + appProps.getProperty("benchmark.enabled"))
    logger.info("Kafka broker IP: " + appProps.getProperty("kafka.broker.ip"))
    logger.info("Kafka broker port: " + appProps.getProperty("kafka.broker.port"))
    logger.info("Kafka input topic: " + appProps.getProperty("kafka.topic.input"))
    logger.info("Kafka output topic: " + appProps.getProperty("kafka.topic.output"))

    // Creating Spark objects
    val conf = new SparkConf()
      .setAppName(appProps.getProperty("spark.app.name"))
      .setMaster("local[*]")
    val spark = SparkSession.builder()
      .appName(appProps.getProperty("spark.app.name"))
      .config("spark.streaming.concurrentJobs", 3)
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    // --------------- Online Stream Processing ---------------

    // Read from Kafka Stream
    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", appProps.getProperty("kafka.broker.ip") + ":" + appProps.getProperty("kafka.broker.port"))
      .option("failOnDataLoss", value = false)
      .option("subscribe", appProps.getProperty("kafka.topic.input"))
      .option("startingOffsets", "earliest")
      .load()
    val datasetDf = inputDf.selectExpr("CAST(value AS STRING)").toDF()

    val regex_str = """^(\w{3}\s+\d+\s\d{2}:\d{2}:\d{2})\s(\S+)\s(\S+)\[(\d+)]:\s(.*)"""
    val df = datasetDf
      .withColumn("timestamp", regexp_extract($"value", regex_str, 1))
      .withColumn("hostname", regexp_extract($"value", regex_str, 2))
      .withColumn("appname", regexp_extract($"value", regex_str, 3))
      .withColumn("pid", regexp_extract($"value", regex_str, 4))
      .withColumn("message", regexp_extract($"value", regex_str, 5))
      .drop("value")
      .toDF()

    val stopWords: Array[String] = utils.getStopWords()
    val tfidfDf = preprocessingFunctions.batchKafkaMessagesProcess(df, stopWords)

    val asDense = udf((v: Vector) => v.toDense.values.mkString("", ", ", ""))
    val densedTfidfDf = tfidfDf.withColumn("tfidfDenseFeatures", asDense($"tfidfFeatures"))
    val formattedDf = densedTfidfDf.select("timestamp", "hostname", "appname", "pid", "tfidfDenseFeatures")

    val concatenatedDf = formattedDf
      .withColumn("value", concat(col("timestamp"), lit(","), concat(col("hostname"), lit(","), col("appname"), lit(","), col("pid"), lit(","), col("tfidfDenseFeatures"))))
      .drop("timestamp")
      .drop("hostname")
      .drop("appname")
      .drop("pid")
      .drop("tfidfDenseFeatures")

    val qConsoleWriter = concatenatedDf.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)
      .start()

    val qKafkaWriter = concatenatedDf
      .withColumn("value", preprocessingFunctions.exposeRecord()('value))
      .writeStream
      .queryName("Parse Kafka Stream")
      .format("kafka")
      .option("kafka.bootstrap.servers", appProps.getProperty("kafka.broker.ip") + ":" + appProps.getProperty("kafka.broker.port"))
      .option("topic", appProps.getProperty("kafka.topic.output"))
      .option("checkpointLocation", "./checkpoint/parse-query")
      .start()

    qConsoleWriter.awaitTermination(60000)
    qKafkaWriter.awaitTermination(60000)
    logger.success("Bye bye!")
    spark.stop()
  }
}

//         --------------- Offline training of the TF-IDF model ---------------

//         ========================== START ===================================
//
//    val startTrainingTime = System.currentTimeMillis()
//
//    // Read csv dataset file
//    var datasetDf = spark.read.csv("dataset.csv").toDF("timestamp", "hostname", "appname", "pid", "message")
//
//    val tokenizer = new RegexTokenizer()
//      .setInputCol("message")
//      .setOutputCol("tokenizedMessage")
//      .setToLowercase(true)
//    var tokenizedDf = tokenizer.transform(datasetDf)
//
//    // Remove the stop words from the syslog message
//    val stopWords: Array[String] = utils.getStopWords()
//    val stopWordsRemover = new StopWordsRemover()
//      .setInputCol("tokenizedMessage")
//      .setOutputCol("filteredMessage")
//      .setCaseSensitive(false)
//      .setStopWords(stopWords)
//    var filteredDf = stopWordsRemover.transform(tokenizedDf)
//
//    val tf = new CountVectorizer()
//      .setInputCol("filteredMessage")
//      .setOutputCol("tfFeatures")
//      .setVocabSize(125)
//    val tfModel = tf.fit(filteredDf)
//    var tfDf = tfModel.transform(filteredDf)
//
//    tfModel.write.overwrite().save("tf-model")
//    logger.dev("TF model saved.")
//
//    val idf = new IDF()
//      .setInputCol("tfFeatures")
//      .setOutputCol("tfidfFeatures")
//      .setMinDocFreq(2)
//    val idfModel = idf.fit(tfDf)
//
//    idfModel.write.overwrite().save("idf-model")
//    logger.dev("IDF model saved.")
//
//    var tfidfDf = idfModel.transform(tfDf)
//
//    val asDense = udf((v: Vector) => v.toDense)
//    val densedTfidfDf = tfidfDf.withColumn("tfidfDenseFeatures", asDense($"tfidfFeatures"))
//
//    logger.dev("---------------- formattedDf --------------")
//    val formattedDf = densedTfidfDf.select("timestamp", "hostname", "appname", "pid", "tfidfDenseFeatures")
//    formattedDf.printSchema()
//    formattedDf.show(numRows = 5, truncate = false)
//
//    val endTrainingTime = System.currentTimeMillis()
//    val duration = (endTrainingTime - startTrainingTime) / 1000
//    logger.dev("Offline training time: " + duration + " seconds.")
//
//     ========================== END ===================================

