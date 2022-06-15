package netflow

import org.apache.spark.sql.SparkSession

class NetflowOrderingMain {

}

object NetflowOrdering {
  def main(args: Array[String]): Unit = {

    // Creating Spark objects
    val spark = SparkSession.builder()
      .appName("Netflow Ordering Applicatoin")
      .config("spark.streaming.concurrentJobs", 1)
      .getOrCreate()

    import spark.implicits._

    // Read from Kafka Stream
    var inputDf = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.101.41.255:9092")
      .option("failOnDataLoss", false)
      .option("subscribe", "ordering-dev")
      .option("startingOffsets", "earliest")
      .option("enable.auto.commit", "true")
      .load()

    inputDf = inputDf.orderBy("key").coalesce(1)

    inputDf.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.101.41.255:9092")
      .option("topic", "ordering-dev-out")
      .save()

    spark.stop()
  }

    
}

