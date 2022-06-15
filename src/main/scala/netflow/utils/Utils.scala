package netflow.utils

import customLogger.CustomLogger

import java.io._
import java.util
import java.util.Properties


import scala.util.matching.Regex

import org.apache.commons.net.util.SubnetUtils
import org.springframework.security.web.util.matcher.IpAddressMatcher
import com.googlecode.ipv6.IPv6Address
import com.googlecode.ipv6.IPv6AddressRange

/**
 * Utils class. Provides helpers and initialization functions for the rest components.
 *
 * @param appConfigFile          String Path of the app's configuration file.
 * @param ipAnonymizeFile        String Path of the file, where IP subnets to be anonymized are saved.
 * @param kafkaMessageSchemaFile String Path of file that contains the column order of the netflow csv records.
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */
@SerialVersionUID(101L)
class Utils(appConfigFile: String, ipAnonymizeFile: String, kafkaMessageSchemaFile: String) extends Serializable {
  /**
   * Logger instance.
   */
  val logger = new CustomLogger()

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
   * HashSet[String]. Load all IPv6 addresses, that will be anonymized if found in a netflow record.
   */
  private var ipV6ToAnonymize: util.HashSet[String] = new util.HashSet[String]()
  ipV6ToAnonymize.addAll(this.loadIpV6List(ipAnonymizeFile))
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
    logger.info("Loading list with IPv4 addresses that will be anonymized during preprocessing stage from file " + ipListFilename + ".")

    val ipBucket: util.HashSet[String] = new util.HashSet[String]
    try {
      ipBucket.clear()

      var line: String = null
      val bufferedReader: BufferedReader = new BufferedReader(new FileReader(new File(ipListFilename)))
      while ( {
        line = bufferedReader.readLine;
        line != null
      }) {
        line = line.trim

        if (line.contains(",")) {
          // Subnet range
          logger.info("Adding IPv4 addresses from subnet " + line + " in anonymization list.")

          val startIp = line.split(",")(0)
          val lastIp = line.split(",")(1)
          
          if(!Utils.isIpv6(startIp) && !Utils.isIpv6(lastIp)) {
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
        }
        else if (line.contains("/")) {
          // Subnet with mask
          logger.info("Adding IPv4 addresses from subnet " + line + " in anonymization list.")

          val subnetBase: String = line.split("/")(0)
          if(!Utils.isIpv6(subnetBase)) {
            val utils = new SubnetUtils(line)
            utils.getInfo.getAllAddresses.foreach( ipAddr  => {
              ipBucket.add(ipAddr)
            })
          }
        }
        else {
          // Single IP address
          logger.info("Adding IPv4 address " + line + " in anonymization list.")

          if(!Utils.isIpv6(line)) {
            ipBucket.add(line)
          }
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
   * Load a file with IPv6 subnets to be anonymized and add them to a list. During anonymization will be checked if an IP belongs to any of these subnets or not.
   *
   * @param ipListFilename String Path of the name, which contains all IP subnets for anonymization.
   * @return HashSet[String] Contains all IP addresses in the loaded subnets. These will be anonymized if found.
   */
  def loadIpV6List(ipListFilename: String): util.HashSet[String] = {
    logger.info("Loading list with IPv6 addresses that will be anonymized during preprocessing stage from file " + ipListFilename + ".")

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
          logger.info("Adding IPv6 addresses from subnet " + line + " in anonymization list.")

          val startIp = line.split(",")(0)
          val lastIp = line.split(",")(1)

          if(Utils.isIpv6(startIp) && Utils.isIpv6(lastIp)) {
              ipBucket.add(line)
          }
        }
        else if (line.contains("/")) {
          // Subnet with mask
          logger.info("Adding IPv6 addresses from subnet " + line + " in anonymization list.")

          val subnetBase: String = line.split("/")(0)
          if(Utils.isIpv6(subnetBase)) {
            ipBucket.add(line)
          }
        }
        else {
          // Single IP address
          logger.info("Adding IPv6 address " + line + " in anonymization list.")

          if(Utils.isIpv6(line)) {
            ipBucket.add(line)
          }
        }
      }
    } catch {
      case e: IOException =>
        logger.error("IOException. Cannot load file " + ipListFilename + ", that contains IPv6 to anonymize.")
        logger.error(e.getCause)
        logger.error(e.getMessage)
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
   * @param schema   HashMap[String, Integer] An object describing the netflow csv records schema (from loadCsvSchema).
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
   * Returns a HashSet with all IPv6 address that need anonymization.
   *
   * @return HashSet[String] Contains all IPv6 addresses for anonymization.
   */
  def getIpsV6(): util.HashSet[String] = {
    // Get list of IPs (v6) to anonymize
    this.ipV6ToAnonymize
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

/**
 * Static methods
 */
object Utils {
  /**
   * Returns if an IP is v6 or not. If it is not IPv6, it will be IPv4.
   * 
   * @param ipAddress String The IP address to check if it is IPv6 or not.
   * @return Boolean If the requested IP is IPv6
   */
  def isIpv6(ipAddress: String): Boolean = {
    val ipV6Pattern: Regex = "(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))".r
    // ipV6Pattern.matches(ipAddress.stripMargin)
    return ipV6Pattern.findAllIn(ipAddress.stripMargin).size > 0
  }

  /**
   * Returns if an IPv6 is in specific range to be anonymized or not. If requested IP is not IPv6
   * false will be returned.
   * 
   * @param ipAddress String The IPv6 address to check if it is in subnet or not.
   * @param ipv6Set HashSet[String] A Hashset containing all IPv6 subnets to be anonymized.
   * @return Boolean If the requested IPv6 is in range or not.
   */
  def inIpv6Range(ipAddress: String, ipv6Set: util.HashSet[String]): Boolean = {
    if(!isIpv6(ipAddress)) {
      return false;
    }

    val iterator: util.Iterator[String] = ipv6Set.iterator()
    while(iterator.hasNext()) {
      val line: String = iterator.next()

      if (line.contains(",")) {
        println("Check subnet range: " + line)
        val range: IPv6AddressRange = IPv6AddressRange.fromFirstAndLast(
          IPv6Address.fromString(line.split(",")(0)),
          IPv6Address.fromString(line.split(",")(1))
        )
        return range.contains(IPv6Address.fromString(ipAddress))
      }
      else if(line.contains("/")) {
        println("Check subnet: " + line)
        val ipAddressMatcher: IpAddressMatcher = new IpAddressMatcher(line)
        return ipAddressMatcher.matches(ipAddress)
      }
      else {
        println("Check single IP: " + line)
        if(isIpv6(line)) {
          return ipAddress.trim().equalsIgnoreCase(line.trim())
        }
        return false
      }
    }
    return false
  }
}