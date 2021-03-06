package customLogger

/**
 * CustomLogger class. A class, implementing a logger. It uses
 * print and formats the messages, adding a tag before them.
 *
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */
@SerialVersionUID(102L)
class CustomLogger() extends Serializable {
  /**
   * Prints the message with a [DEBUG] tag before it.
   *
   * @param msg String The message to be print.
   */
  def debug(msg: String): Unit = {
    println("[DEBUG] " + msg)
  }

  /**
   * Prints the message with a [INFO] tag before it.
   *
   * @param msg String The message to be print.
   */
  def info(msg: String): Unit = {
    println("[INFO] " + msg)
  }

  /**
   * Prints the message with a [WARN] tag before it.
   *
   * @param msg String The message to be print.
   */
  def warn(msg: String): Unit = {
    println("[WARN] " + msg)
  }

  /**
   * Prints the message with a [ERROR] tag before it.
   *
   * @param msg String The message to be print.
   */
  def error(msg: String): Unit = {
    println("[ERROR] " + msg)
  }

  /**
   * Prints the message with a [DEV] tag before it.
   *
   * @param msg String The exception to be print.
   */
  def dev(msg: String): Unit = {
    println("[DEV] " + msg)
  }

  /**
   * Prints the message with a [SUCCESS] tag before it.
   *
   * @param msg String The exception to be print.
   */
  def success(msg: String): Unit = {
    println("[SUCCESS] " + msg)
  }
  /**
   * Prints an exception with a [ERROR] tag before it.
   *
   * @param msg Throwable The exception to be print.
   */
  def error(msg: Throwable): Unit = {
    println("[ERROR] " + msg)
  }
}
