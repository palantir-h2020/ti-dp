package spark

import com.google.gson.Gson
import logger.Logger
import org.apache.spark.sql.functions.udf
import netflow.utils.{RequestBody, ResponseBody}
import scalaj.http.{Http, HttpOptions}

import java.io.{FileWriter}

/**
 * NetflowPreprocessingFunctions class. Contains all methods that will be
 * used for preprocessing ingested raw netflow data to Kafka. Thr available
 * operations of this class are the following: anonymize a netflow record,
 * preprocess a netflow record. Anonymization is happening in a separate
 * service, which exposes an API for sending IP addresses for anonymization
 * and responds with the obfuscated IP address. Preprocessing operation creates
 * some new features based on the existing ones of the netflow data.
 *
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */
@SerialVersionUID(100L)
class NetflowPreprocessingFunctions() extends Serializable {
  /**
   * Logger instance
   */
  val logger = new Logger()

  /**
   * Index of column ipkt in netflow data.
   */
  val pktInIdx = 11
  /**
   * Index of column opkt in netflow data.
   */
  val pktOutIdx = 13
  /**
   * Index of column ibyt in netflow data.
   */
  val byteInIdx = 12
  /**
   * Index of column obyt in netflow data.
   */
  val byteOutIdx = 14
  /**
   * Index of column da in netflow data.
   */
  val dstPortIdx = 6
  /**
   * Index of column pr in netflow data.
   */
  val protocolIdx = 7
  /**
   * Index of column flg in netflow data.
   */
  val tcpFlagsIdx = 8

  /**
   * List[Int] with some common services ports. The list includes the following ports:
   * FTP(20,21), SSH(22), Telnet(23), SMTP(25), DNS(53), DHCP(67,68), TFTP(69), HTTP(80), POP3(110),
   * NNTP(119), NTP(123), IMAP4(143), SNMP(161), LDAP(389), HTTPS(443), IMAPS(993), RADIUS(1812), AIM(5190)
   */
  val commonPorts: List[Int] = List(20, 21, 22, 23, 25, 53, 67, 68, 69, 80, 110, 119, 123, 143, 161, 389, 443, 993, 1812, 5190)

  /**
   * Anonymize a netflow record. Only specific (internal) IP addresses will be anonymized. These addresses
   * are defined in ip-anonymize.txt file. The names of the columns of the netflow that must be anonymized
   * are configured in netflow-preprocessing.properties file or through Dockerfile if the app is running
   * containerized. For each of these columns, the app checks if it belongs in the list with the IP addresses
   * that must be anonymized. If it belongs, an HTTP request is send to anonymization service. This service
   * anonymizes the given IP and returns the obfuscated IP among other information and some stats about the
   * query. The anonymized IP address replaces the original one and the netflow record is returned to the
   * main Spark Streaming App in order to be sent back to Kafka after all Spark operations are completed. If
   * app's benchmark mode is enabled both HTTP status code and response body of the anonymization service will
   * be stored in benchmark.txt file.
   *
   * @param anonymizationEndpoint String Anonymization service endpoint, where the HTTP request will be sent.
   *                              This must be in format http://IP:PORT/anonymize.
   * @param colsIdxToAnonymize Array[Int] An array containing all indexes of columns that their values must be
   *                           sent for anonymization.
   * @param ipList HashSet[String] A set containing all IP addresses that must be anonymized. This is created after
   *               loading ip-anonymize.txt file which contains all subnets that their IPs must be anonymized. From
   *               these subnets, a HashSet of all IP addresses has been created and loaded.
   * @param isBenchmarkEnabled Boolean Defines if the app is running in benchmark mode or not. If it is running in
   *                           benchmark mode, anonymization service responds will be dumped in a file,
   *                           named benchmark.txt, for analysis.
   * @return String The input netflow record, but with the required IP addresses replaced with the obfuscated ones.
   */
  def anonymizeRecord(anonymizationEndpoint: String, colsIdxToAnonymize: Array[Int], ipList: java.util.HashSet[String], isBenchmarkEnabled: Boolean) = udf ((kafkaRecord: String) => {
    val recordCols = kafkaRecord.split(",")
    colsIdxToAnonymize.foreach(colIdx => {
      val col = recordCols(colIdx).trim

      // Check if this IP must be anonymized
      if(ipList.contains(col)) {
        // Create a Request object and convert it to JSON object
        val reqBody = new RequestBody(col)
        val reqBodyJson = new Gson().toJson(reqBody)

        val anonymizeRoute = anonymizationEndpoint
        // Create and send HTTP request object
        val response = Http(anonymizeRoute)
          .postData(reqBodyJson)
          .header("Content-Type", "application/json")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(10000)).asString

        // Get Response
        val resCode = response.code
        val resContent = response.body

        // Save response to file, if in benchmark mode.
        if(isBenchmarkEnabled) {
          val fw = new FileWriter("benchmark.txt", true)
          try {
            fw.write(resContent + " " + resCode + "\n")
          }
          finally fw.close()
        }

        val gson = new Gson
        val resContentJson = gson.fromJson(resContent, classOf[ResponseBody])

        recordCols(colIdx) = resContentJson.getObfuscatedIp().trim
      }
    })
    val anonymizedRecord = recordCols.mkString(",")

    anonymizedRecord
  })

  /**
   * Preprocess a netflow record. Some new extra features will be created from the existing ones.
   * The created new features are the following: <br>
   * tpkt: Total packets of the netflow. If the netflow is bidirectional this is the sum of ipkt and opkt.
   * Otherwise, this is equal to ipkt <br>
   * tbyt: Total bytes of the netflow. If the netflow is bidirectional this is the sum of ibyt and obyt.
   * Otherwise, this is equal to ibyt <br>
   * cp: Check if destination port is a port used from common services or not.
   * Value is 1 if destination port is common, 0 otherwise <br>
   * prtcp: Checks if protocol is TCP. Value is 1 if it is, 0 otherwise. <br>
   * prudp: Checks if protocol is UDP. Value is 1 if it is, 0 otherwise. <br>
   * pricmp: Checks if protocol is ICMP. Value is 1 if it is, 0 otherwise. <br>
   * prigmp: Checks if protocol is IGMP. Value is 1 if it is, 0 otherwise. <br>
   * prother: Checks if protocol is not TCP, UDP, ICMP or IGMP. Value is 1 if it is none of them, 0 otherwise. <br>
   * flga: Checks if TCP flags of the netflow contain letter A.
   * Value is 1 if it contains it or if TCP flag is equal to X, 0 otherwise <br>
   * flgs: Checks if TCP flags of the netflow contain letter S.
   * Value is 1 if it contains it or if TCP flag is equal to X, 0 otherwise <br>
   * flgf: Checks if TCP flags of the netflow contain letter F.
   * Value is 1 if it contains it or if TCP flag is equal to X, 0 otherwise <br>
   * flgr: Checks if TCP flags of the netflow contain letter R.
   * Value is 1 if it contains it or if TCP flag is equal to X, 0 otherwise <br>
   * flgp: Checks if TCP flags of the netflow contain letter P.
   * Value is 1 if it contains it or if TCP flag is equal to X, 0 otherwise <br>
   * flgu: Checks if TCP flags of the netflow contain letter U.
   * Value is 1 if it contains it or if TCP flag is equal to X, 0 otherwise <br>
   *
   * @return Netflow record, with the new created features added in the right of the old columns.
   */
  def preprocessRecord() = udf ((kafkaRecord: String) => {
    // Split record from kafka
    var recordCols = kafkaRecord.split(",")

    // Preprocessing Function #1: Count total packets & total bytes.
    // If netflow is not bidirectional, this will be the same with input packets & bytes.
    val pktTotal = ((recordCols(pktInIdx).trim).toInt + (recordCols(pktOutIdx).trim).toInt)
    val bytTotal = ((recordCols(byteInIdx).trim).toDouble + (recordCols(byteOutIdx).trim).toDouble)

    // Preprocessing Function #2: Check if destination port is a port used from common services or not.
    // False: 0, True: 1
    var isCommonPort = 0
    if(commonPorts.contains(recordCols(dstPortIdx).trim.toInt)) {
      isCommonPort = 1
    }

    // Preprocessing Function #3: One-Hot Encoding in network protocol column.
    // False: 0, True: 1
    var isProtTCP = 0
    var isProtUDP = 0
    var isProtICMP = 0
    var isProtIGMP = 0
    var isProtOther = 0

    if(recordCols(protocolIdx).trim.equalsIgnoreCase("TCP")) {
      isProtTCP = 1
    }
    else if(recordCols(protocolIdx).trim.equalsIgnoreCase("UDP")) {
      isProtUDP = 1
    }
    else if(recordCols(protocolIdx).trim.equalsIgnoreCase("ICMP")) {
      isProtICMP = 1
    }
    else if(recordCols(protocolIdx).trim.equalsIgnoreCase("IGMP")) {
      isProtIGMP = 1
    }
    else {
      isProtOther = 1
    }

    // Preprocessing Function #4: One-Hot Encoding in flags column.
    // False: 0, True: 1
    var flgHasA = 0
    var flgHasS = 0
    var flgHasF = 0
    var flgHasR = 0
    var flgHasP = 0
    var flgHasU = 0

    if(recordCols(tcpFlagsIdx).trim.toUpperCase.contains("A") ||
      recordCols(tcpFlagsIdx).trim.toUpperCase.contains("X")) {
      flgHasA = 1
    }
    if(recordCols(tcpFlagsIdx).trim.toUpperCase.contains("S") ||
      recordCols(tcpFlagsIdx).trim.toUpperCase.contains("X")) {
      flgHasS = 1
    }
    if(recordCols(tcpFlagsIdx).trim.toUpperCase.contains("F") ||
      recordCols(tcpFlagsIdx).trim.toUpperCase.contains("X")) {
      flgHasF = 1
    }
    if(recordCols(tcpFlagsIdx).trim.toUpperCase.contains("R") ||
      recordCols(tcpFlagsIdx).trim.toUpperCase.contains("X")) {
      flgHasR = 1
    }
    if(recordCols(tcpFlagsIdx).trim.toUpperCase.contains("P") ||
      recordCols(tcpFlagsIdx).trim.toUpperCase.contains("X")) {
      flgHasP = 1
    }
    if(recordCols(tcpFlagsIdx).trim.toUpperCase.contains("U") ||
      recordCols(tcpFlagsIdx).trim.toUpperCase.contains("X")) {
      flgHasU = 1
    }

    // Append new features
    //tpkt
    recordCols :+= pktTotal.toString
    //tbyt
    recordCols :+= bytTotal.toString
    //cp
    recordCols :+= isCommonPort.toString
    //prtcp
    recordCols :+= isProtTCP.toString
    //prudp
    recordCols :+= isProtUDP.toString
    //pricmp
    recordCols :+= isProtICMP.toString
    //prigmp
    recordCols :+= isProtIGMP.toString
    //prother
    recordCols :+= isProtOther.toString
    //flga
    recordCols :+= flgHasA.toString
    //flgs
    recordCols :+= flgHasS.toString
    //flgf
    recordCols :+= flgHasF.toString
    //flgr
    recordCols :+= flgHasR.toString
    //flgp
    recordCols :+= flgHasP.toString
    //flgu
    recordCols :+= flgHasU.toString

    // Create new record and return
    val preprocessedRecord = recordCols.mkString(",")
    preprocessedRecord
  })
}