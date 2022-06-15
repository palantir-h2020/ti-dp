package netflow.utils

/**
 * RequestBody class. A class describing the request body of the IP anonymization
 * HTTP request. The body represents an object, that will be converted in JSON format.
 * The only field in the JSON is a string with the key IpAddr.
 *
 * @param IpAddr String The IP address to be sent for anonymization.
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */
@SerialVersionUID(103L)
class RequestBody(private var IpAddr: String) extends Serializable {

  /**
   * Getter for the IpAddr param.
   *
   * @return String The IP address to be anonymized.
   */
  def getIpAddr(): String = {
    return IpAddr
  }

  /**
   * Setter for the IpAddr param.
   *
   * @param ip String The IP, that will be anonymized.
   */
  def setIpAddr(ip: String): Unit = {
    IpAddr = ip
  }
}
