package h2.queries

import helper.{JdbcConnection, MainHelper}
import java.util.Properties


object H2QueryEvaluation {
  val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def main(args: Array[String]): Unit = {
    MainHelper.argsCheck(args, 4)
    val spark = MainHelper.createSparkSession

    val jdbcConnection = JdbcConnection(args(0), connectionProperties)
    jdbcConnection.createTableTempView("source_date", spark)
    jdbcConnection.createTableTempView("news_word", spark)
    jdbcConnection.createTableTempView("word_frequency", spark)
    jdbcConnection.createTableTempView("aggregated_date", spark)
    jdbcConnection.createTableTempView("aggregated_word_frequency", spark)

    val frequencyPerSourceQuery = "SELECT NW.word, WF.frequency, SD.DATE, SD.SOURCE " +
      "FROM WORD_FREQUENCY WF " +
      "JOIN NEWS_WORD NW on WF.NEWS_WORD_ID = NW.ID " +
      "JOIN SOURCE_DATE SD on WF.SOURCE_DATE_ID = SD.ID " +
      "WHERE NW.word = '" + args(1).toLowerCase + "' " +
      "AND SD.DATE BETWEEN '" + args(2) + "' AND '" + args(3) + "' " +
      "ORDER BY SD.DATE;"
    val frequencyPerSourceDF = spark.sql(frequencyPerSourceQuery)
    frequencyPerSourceDF.show(frequencyPerSourceDF.count.toInt)

    val frequencyPerDayQuery = "SELECT NW.word, SUM(WF.frequency), SD.DATE " +
      "FROM WORD_FREQUENCY WF " +
      "JOIN NEWS_WORD NW on WF.NEWS_WORD_ID = NW.ID " +
      "JOIN SOURCE_DATE SD on WF.SOURCE_DATE_ID = SD.ID " +
      "WHERE NW.word = '" + args(1).toLowerCase + "' " +
      "AND SD.DATE BETWEEN '" + args(2) + "' AND '" + args(3) + "' " +
      "GROUP BY SD.DATE, NW.word " +
      "ORDER BY SD.DATE;"
    val frequencyPerDayDF = spark.sql(frequencyPerDayQuery)
    frequencyPerDayDF.show(frequencyPerDayDF.count.toInt)

    val frequencyPerDayQueryPreAggregation = "SELECT NW.word, AWF.frequency, AWF.date " +
      "FROM AGGREGATED_WORD_FREQUENCY AWF " +
      "JOIN NEWS_WORD NW on AWF.NEWS_WORD_ID = NW.ID " +
      "WHERE NW.word = 'ukraine' " +
      "AND AWF.DATE BETWEEN '2022-05-20' AND '2022-09-21' " +
      "GROUP BY AWF.DATE, NW.word, AWF.frequency " +
      "ORDER BY AWF.DATE;"
    val frequencyPerDayPreAggregationDF = spark.sql(frequencyPerDayQueryPreAggregation)
    frequencyPerDayPreAggregationDF.show(frequencyPerDayPreAggregationDF.count.toInt)

    spark.stop()
  }
}
