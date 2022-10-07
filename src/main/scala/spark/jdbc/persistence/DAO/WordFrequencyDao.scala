package spark.jdbc.persistence.DAO

import helper.JdbcConnection
import org.apache.spark.sql.{SaveMode, SparkSession}
import persistence.DbClasses.WordFrequency
import java.util.Properties


case class WordFrequencyDao(spark: SparkSession, connectionUrl: String) {
  private val tableName = "word_frequency"
  private val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def saveIfNotExists(wordFrequency: WordFrequency): Unit = if(findId(wordFrequency).isEmpty) save(wordFrequency)


  def save(wordFrequency: WordFrequency): Unit = {
    try {
      val sourceDateDF = spark.createDataFrame(Seq(wordFrequency))
        .toDF("frequency", "news_word_id", "source_date_id")
      sourceDateDF.write.mode(SaveMode.Append).jdbc(connectionUrl, tableName, connectionProperties)
    } catch {
      case e: Exception => println(s"Error trying to add wordFrequency: $wordFrequency} ${e.getCause}")
    }
  }


  def findId(wordFrequency: WordFrequency): Option[Int] = {
    try {
      val wordFrequencyDF = JdbcConnection(connectionUrl, connectionProperties).getTableAsDataframe(tableName, spark)
      val queryResult = wordFrequencyDF.select("id")
        .where(wordFrequencyDF("frequency") === wordFrequency.frequency &&
          wordFrequencyDF("news_word_id") === wordFrequency.newsWordsId &&
          wordFrequencyDF("source_date_id") === wordFrequency.sourceDateId)
      if(queryResult.count.toInt > 0) return Some(queryResult.collect.toList.map(el=>el(0).toString.toInt).head)
    } catch {
      case e: Exception => println(s"Error trying to find wordFrequency: ${wordFrequency.toString} ${e.getCause}")
    }
    None
  }
}
