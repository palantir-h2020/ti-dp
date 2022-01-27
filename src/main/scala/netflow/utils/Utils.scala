package netflow.utils

import logger.Logger

import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.FileReader
import java.io.IOException
import java.util
import java.util.Properties
import org.apache.commons.net.util.SubnetUtils

/**
 * Utils class. Provides helpers and initialization functions for the rest components.
 *
 * @param appConfigFile String Path of the app's configuration file.
 * @param ipAnonymizeFile String Path of the file, where IP subnets to be anonymized are saved.
 * @param kafkaMessageSchemaFile String Path of file that contains the column order of the netflow csv records.
 *
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */
@SerialVersionUID(101L)
class Utils (appConfigFile: String, ipAnonymizeFile: String, kafkaMessageSchemaFile: String) extends Serializable {
  /**
   * Logger instance.
   */
  val logger = new Logger()

  /**
   * Properties. Load all required app's properties.
   */
  private val appProps: Properties = this.loadProperties(appConfigFile)
  /**
   * HashSet[String]. Load all IP addresses, that will be anonymized if found in a netflow record.
   */
  private var ipToAnonymize: util.HashSet[String] = new util.HashSet[String]()
  ipToAnonymize.addAll(this.loadIpList(ipAnonymizeFile))
  /**
   * HashMap[String, Integer]. Load csv schema of Kafka records.
   */
  private val csvSchema: util.HashMap[String, Integer] = this.loadCsvSchema(kafkaMessageSchemaFile)
  /**
   * Array[Int]. Load columns' indexes in csv schema, that will be anonymized.
   */
  private val colIndexesToAnonymize: Array[Int] = this.loadColsToAnonymize(
      csvSchema,
      appProps.getProperty("anonymization.columns").split(",")
  )

  /**
   * Load properties file.
   *
   * @param propertiesFilename String Path of the app's properties file.
   * @return Properties A properties object, storing all app's properties.
   */
  def loadProperties(propertiesFilename: String): Properties = {
    logger.info("Loading properties from file " + propertiesFilename + ".")

    val appProps = new Properties
    try {
      appProps.load(new FileInputStream(new File(propertiesFilename)))
    }
    catch {
      case e: IOException =>
        logger.error("Cannot initialize app properties from file " + propertiesFilename + ". Loading some default values.")
        logger.error(e.getCause)
        logger.error(e.getMessage)

        appProps.setProperty("kafka.broker.ip", "1.1.1.1")
        appProps.setProperty("kafka.broker.port", Integer.toString(9092))
        appProps.setProperty("kafka.topic.input", "netflow-raw")
        appProps.setProperty("kafka.topic.output", "netflow-anonymized,netflow-preprocessed,netflow-anonymized-preprocessed")
        appProps.setProperty("spark.master.url", "local[*]")
        appProps.setProperty("spark.streaming.interval.seconds", Integer.toString(5))
        appProps.setProperty("spark.app.name", "Palantir Preprocessing Netflow Application")
        appProps.setProperty("anonymization.endpoint", "http://localhost:8100/anonymize")
        appProps.setProperty("anonymization.rolling.batch", Integer.toString(10))
        appProps.setProperty("anonymization.columns", "sa,da")
        appProps.setProperty("files.anonymization.path", "ip-anonymize.txt")
        appProps.setProperty("files.message.schema.path", "netflow-raw-csv-schema.txt")
    }

    appProps
  }

  /**
   * Load a file with IP subnets to be anonymized. Add all IPs in these subnets in a list.
   *
   * @param ipListFilename String Path of the name, which contains all IP subnets for anonymization.
   * @return HashSet[String] Contains all IP addresses in the loaded subnets. These will be anonymized if found.
   */
  def loadIpList(ipListFilename: String): util.HashSet[String] = {
    logger.info("Loading list with IP addresses that will be anonymized during preprocessing stage from file " + ipListFilename + ".")

    val ipBucket: util.HashSet[String] = new util.HashSet[String]
    try {
      ipBucket.clear()

      var line: String = null
      val bufferedReader: BufferedReader = new BufferedReader(new FileReader(new File(ipListFilename)))
      while ({
          line = bufferedReader.readLine; line != null
      }) {
        line = line.trim

        if (line.contains(",")) {
          // Subnet range
          logger.info("Adding IP addresses from subnet " + line + " in anonymization list.")

          val startIp = line.split(",")(0)
          val lastIp = line.split(",")(1)
          val ipPrefix = startIp.split("\\.")(0) + "." + startIp.split("\\.")(1) + "." + startIp.split("\\.")(2) + "."

          val startIpLastOctad = startIp.split("\\.")(3).toInt
          val lastIpLastOctad = lastIp.split("\\.")(3).toInt

          if (startIpLastOctad <= lastIpLastOctad) for (i <- startIpLastOctad to lastIpLastOctad) {
            ipBucket.add(ipPrefix + Integer.toString(i))
          }
          else for (i <- lastIpLastOctad to startIpLastOctad) {
            ipBucket.add(ipPrefix + Integer.toString(i))
          }
        }
        else if (line.contains("/")) {
          // Subnet with mask
          logger.info("Adding IP addresses from subnet " + line + " in anonymization list.")

          val utils = new SubnetUtils(line)
          utils.getInfo.getAllAddresses.foreach( ipAddr  => {
            ipBucket.add(ipAddr)
          })
        }
        else {
          // Single IP address
          logger.info("Adding IP address " + line + " in anonymization list.")

          ipBucket.add(line)
        }
      }
    } catch {
      case e: IOException =>
        logger.error("IOException. Cannot load file " + ipListFilename + ", that contains IP to anonymize. Returning subnets 192.168.1.0/24 and 10.0.19.0/24.")
        logger.error(e.getCause)
        logger.error(e.getMessage)

        ipBucket.clear()
        for (i <- 0 until 255) {
          ipBucket.add("192.168.1." + Integer.toString(i))
          ipBucket.add("10.0.19." + Integer.toString(i))
        }
    }
    ipBucket
  }

  /**
   * Load CSV schema of raw netflow records ingested to Kafka.
   *
   * @param schemaFilename String Path of file, which contains schema of the raw csv netflow records.
   * @return HashMap[String, Integer] An object describing the netflow csv records schema.
   */
  def loadCsvSchema(schemaFilename: String): util.HashMap[String, Integer] = {
    logger.info("Loading CSV schema for incoming messages from file " + schemaFilename + ".")

    val schemaMap = new util.HashMap[String, Integer]
    var csvSchema: String = null
    try {
      val bufferedReader = new BufferedReader(new FileReader(new File(schemaFilename)))
      csvSchema = bufferedReader.readLine

      logger.info("Successfully loaded csv schema file " + schemaFilename + " for incoming data.")
    } catch {
      case e: IOException =>
        logger.error("Could not load csv schema file " + schemaFilename + " for incoming data from Kafka. A default schema will be used.")
        csvSchema = "ts,te,td,sa,da,sp,dp,pr,flg,fwd,stos,ipkt,ibyt,opkt,obyt,in,out,sas,das,smk,dmk,dtos,dir,nh,nhb,svln,dvln,ismc,odmc,idmc,osmc,mpls1,mpls2,mpls3,mpls4,mpls5,mpls6,mpls7,mpls8,mpls9,mpls10,cl,sl,al,ra,eng,exid,tr"
    }

    var counter = 0
    for (col <- csvSchema.split(",")) {
      schemaMap.put(col, counter)
      counter += 1
    }
    schemaMap
  }

  /**
   * Load columns' indexes, that must search for IP addresses that may need anonymization.
   *
   * @param schema HashMap[String, Integer] An object describing the netflow csv records schema (from loadCsvSchema).
   * @param colNames Array[String] An array containing all column names, that indexes must be found.
   * @return Array[Int] An array, which contains indexes of all columns that must be anonymized.
   */
  def loadColsToAnonymize(schema: util.HashMap[String, Integer], colNames: Array[String]): Array[Int] = {
    val colIndexes = new Array[Int](colNames.length)
    for (i <- 0 until colNames.length) {
      colIndexes(i) = schema.get(colNames(i))
    }
    colIndexes
  }

  /**
   * Return app properties.
   *
   * @return Properties An object storing all app properties.
   */
  def getAppProps(): Properties = {
    // Get app configuration
    this.appProps
  }

  /**
   * Returns a HashSet with all IP address that need anonymization.
   *
   * @return HashSet[String] Contains all IP addresses for anonymization.
   */
  def getIps(): util.HashSet[String] = {
    // Get list of IPs to anonymize
    this.ipToAnonymize
  }

  /**
   * Returns csv columns order of Kafka messages' value.
   *
   * @return HashMap[String, Integer] An object describing the netflow csv records schema.
   */
  def getKafkaMessagesSchema(): util.HashMap[String, Integer] = {
    // Get schema of incoming Kafka messages
    this.csvSchema
  }

  /**
   * Return indexes of column that must be anonymized.
   *
   * @return Array[Int] An array containing all column indexes that will be anonymized.
   */
  def getColsToAnonymize(): Array[Int] = {
    // Get list of columns, that contain IP addresses to be anonymized
    this.colIndexesToAnonymize
  }
}
