package spark.jdbc.persistence.DAO

import helper.JdbcConnection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import persistence.DbClasses.AggregatedWordFrequency
import java.sql.Date
import java.util.Properties


case class PreAggregateArticleDao(spark: SparkSession, connectionUrl: String) {
  val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")

  private val aggregatedWordFrequencyDao = AggregatedWordFrequencyDao(spark, connectionUrl)


  def preAggregate(): Unit = {
    try {
      val jdbcConnection = JdbcConnection(connectionUrl, connectionProperties)
      val newsWordDF = jdbcConnection.getTableAsDataframe("news_word", spark)
      val sourceDateDF = jdbcConnection.getTableAsDataframe("source_date", spark)
      val wordFrequencyDF = jdbcConnection.getTableAsDataframe("word_frequency", spark)

      val distinctWords = newsWordDF.collect()


      for(distinctWord <- distinctWords) {
        val id = distinctWord(0)
        val word = distinctWord(1)

        val aggregatedWordFrequencyArr = wordFrequencyDF.as("wf")
          .join(newsWordDF.as("nw"), wordFrequencyDF("news_word_id") ===  newsWordDF("id"),"full")
          .join(sourceDateDF.as("sd"), wordFrequencyDF("source_date_id") === sourceDateDF("id"), "full")
          .where(newsWordDF("word") === word)
          .select("nw.word", "wf.frequency", "sd.date")
          .groupBy(sourceDateDF("date"), newsWordDF("word"))
          .agg(sum("wf.frequency"))
          .orderBy(sourceDateDF("date"))
          .collect()
        //TODO collect weg

        //TODO for schleife weg und DF auf einmal saven
        for(el <- aggregatedWordFrequencyArr) {
          val aggregatedWordFrequency = AggregatedWordFrequency(
            el(2).toString.toInt,
            id.toString.toInt,
            Date.valueOf(el(0).toString)
          )
          aggregatedWordFrequencyDao.save(aggregatedWordFrequency)
        }
      }
    } catch {
      case e: Exception => println(s"Error trying to add preAggregateArticle ${e.getCause}")
        e.printStackTrace()
    }
  }
}
