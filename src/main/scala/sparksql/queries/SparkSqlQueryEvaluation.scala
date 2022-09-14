package sparksql.queries

import helper.{ArticleExtractor, JdbcConnection, MainHelper}
import org.apache.spark.sql.functions._

import java.util.Properties
//import Extraction.ArticleExtractor


object SparkSqlQueryEvaluation {
  val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def main(args: Array[String]): Unit = {
    MainHelper.argsCheck(args, 4)
    val spark = MainHelper.createSparkSession

    val jdbcConnection = JdbcConnection(args(0), connectionProperties)
    val sourceDate = jdbcConnection.createTable("source_date", spark)
    val newsWord = jdbcConnection.createTable("news_word", spark)
    val wordFrequency = jdbcConnection.createTable("word_frequency", spark)

    val basicQuery = wordFrequency.as("wf")
      .join(newsWord.as("nw"), wordFrequency("news_word_id") ===  newsWord("id"),"full")
      .join(sourceDate.as("sd"), wordFrequency("source_date_id") ===  sourceDate("id"),"full")
      .where(newsWord("word") === args(1).toLowerCase && sourceDate("date") >= args(2) && sourceDate("date") <= args(3))

    val frequencyPerSourceDataSet = basicQuery
      .select("nw.word", "wf.frequency", "sd.date", "sd.source")
      .orderBy(sourceDate("date"))
    frequencyPerSourceDataSet.show(frequencyPerSourceDataSet.count.toInt)

    val frequencyPerDayDataSet = basicQuery
      .select("nw.word", "wf.frequency", "sd.date")
      .groupBy(sourceDate("date"), newsWord("word"))
      .agg(sum("wf.frequency"))
      .orderBy(sourceDate("date"))
    frequencyPerDayDataSet.show(frequencyPerDayDataSet.count.toInt)

    spark.stop()
  }
}
