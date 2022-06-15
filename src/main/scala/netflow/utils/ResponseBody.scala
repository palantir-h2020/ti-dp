package netflow.utils

/**
 * ResponseBody class. A class describing the body of the response of the IP anonymization
 * HTTP request. The body represents an object, that will be converted in JSON format. The
 * following JSON keys will exist in this JSON: obfuscatedIp, originalIp, result, execution_time.
 *
 * @param obfuscatedIp   String The obfuscated IP address, after anonymization process.
 * @param originalIp     String The original IP address, that was sent for anonymization.
 * @param result         String Status of the query. For example "success" or "error".
 * @param execution_time Float Time needed (in seconds) for executing the anonymization query.
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */
@SerialVersionUID(104L)
class ResponseBody(
                    private var obfuscatedIp: String,
                    private var originalIp: String,
                    private var result: String,
                    private var execution_time: Float,
                  ) extends Serializable {

  /**
   * Getter for the obfuscatedIp param.
   *
   * @return String The obfuscated IP address.
   */
  def getObfuscatedIp(): String = {
    return obfuscatedIp
  }

  /**
   * Getter for the originalIp param.
   *
   * @return String The IP address to be anonymized.
   */
  def getOriginalIp(): String = {
    return originalIp
  }

  /**
   * Getter for the result param.
   *
   * @return String Status about the anonymization query.
   */
  def getResult(): String = {
    return result
  }

  /**
   * Getter for the execution_time param.
   *
   * @return Float Time needed (in seconds) for anonymization.
   */
  def getExecutionTime(): Float = {
    return execution_time
  }

  /**
   * Setter for the obfuscatedIp param.
   *
   * @param ip String Obfuscated IP address.
   */
  def setObfuscatedIp(ip: String): Unit = {
    obfuscatedIp = ip
  }

  /**
   * Setter for the originalIp param.
   *
   * @param ip String Original non-anonymized IP address.
   */
  def setOriginalIp(ip: String): Unit = {
    originalIp = ip
  }

  /**
   * Setter for the result param.
   *
   * @param res String Status about anonymization query.
   */
  def setResult(res: String): Unit = {
    result = res
  }

  /**
   * Setter for the execution_time param.
   *
   * @param exec_time Float Execution time (in seconds) needed for anonymization query.
   */
  def setExecutionTime(exec_time: Float): Unit = {
    execution_time = exec_time
  }

}
