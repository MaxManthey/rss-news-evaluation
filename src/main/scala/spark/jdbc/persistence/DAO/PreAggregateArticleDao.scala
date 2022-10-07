package spark.jdbc.persistence.DAO

import helper.JdbcConnection
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.sum
import java.util.Properties


case class PreAggregateArticleDao(spark: SparkSession, connectionUrl: String) {
  val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def preAggregate(): Unit = {
    try {
      val jdbcConnection = JdbcConnection(connectionUrl, connectionProperties)
      val newsWordDF = jdbcConnection.getTableAsDataframe("news_word", spark)
      val sourceDateDF = jdbcConnection.getTableAsDataframe("source_date", spark)
      val wordFrequencyDF = jdbcConnection.getTableAsDataframe("word_frequency", spark)

      val distinctWords = newsWordDF.collect()

      for(distinctWord <- distinctWords) {
        val word = distinctWord(1)

        val aggregatedWordFrequencyDF = wordFrequencyDF.as("wf")
          .join(newsWordDF.as("nw"), wordFrequencyDF("news_word_id") ===  newsWordDF("id"),"full")
          .join(sourceDateDF.as("sd"), wordFrequencyDF("source_date_id") === sourceDateDF("id"), "full")
          .where(newsWordDF("word") === word)
          .select("nw.id", "wf.frequency", "sd.date")
          .groupBy(newsWordDF("id").as("news_word_id"), sourceDateDF("date"))
          .agg(sum("wf.frequency"))
          .withColumnRenamed("sum(wf.frequency)", "frequency")
          .orderBy(sourceDateDF("date"))

        aggregatedWordFrequencyDF
          .write
          .mode(SaveMode.Append)
          .jdbc(connectionUrl, "AGGREGATED_WORD_FREQUENCY", connectionProperties)
      }
    } catch {
      case e: Exception => println(s"Error trying to add preAggregateArticle ${e.getCause}")
        e.printStackTrace()
    }
  }
}
