package spark

import customLogger.CustomLogger
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, CountVectorizerModel, IDFModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


/**
 * SyslogPreprocessingFunctions class.
 *
 * @author Space Hellas S.A.
 * @version 1.0-SNAPSHOT
 * @since 1.0-SNAPSHOT
 */

@SerialVersionUID(100L)
class SyslogPreprocessingFunctions() extends Serializable {

  /**
   * Logger instance
   */
  val logger = new CustomLogger()

  def batchKafkaMessagesProcess(batchDf: DataFrame, stopWords: Array[String]): DataFrame = {

    val tokenizer = new RegexTokenizer()
      .setInputCol("message")
      .setOutputCol("tokenizedMessage")
      .setToLowercase(true)
    val tokenizedDf = tokenizer.transform(batchDf)

    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("tokenizedMessage")
      .setOutputCol("filteredMessage")
      .setCaseSensitive(false)
      .setStopWords(stopWords)
    val filteredDf = stopWordsRemover.transform(tokenizedDf)

    val tfModel: CountVectorizerModel = CountVectorizerModel.load("tf-model")
    val tfDf = tfModel.transform(filteredDf)

    val idfModel: IDFModel = IDFModel.load("idf-model")
    val tfidfDf = idfModel.transform(tfDf)

    tfidfDf
  }

  def exposeRecord(): UserDefinedFunction = udf((kafkaRecord: String) => {

    kafkaRecord
  })

}