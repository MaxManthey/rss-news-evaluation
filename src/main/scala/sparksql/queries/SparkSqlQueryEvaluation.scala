package sparksql.queries

import helper.{JdbcConnection, MainHelper}
import java.util.Properties


object SparkSqlQueryEvaluation {
  val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def main(args: Array[String]): Unit = {
    MainHelper.argsCheck(args, 4)
    val spark = MainHelper.createSparkSession

    val jdbcConnection = JdbcConnection(args(0), connectionProperties)
    val sourceDate = jdbcConnection.getTableAsDataframe("source_date", spark)
    val newsWord = jdbcConnection.getTableAsDataframe("news_word", spark)
    val wordFrequency = jdbcConnection.getTableAsDataframe("word_frequency", spark)
    val aggregatedWordFrequency = jdbcConnection.getTableAsDataframe("aggregated_word_frequency", spark)

    val sparkSqlQueries = SparkSqlQueries(sourceDate, newsWord, wordFrequency, aggregatedWordFrequency)

    val frequencyPerSourceDataSet = sparkSqlQueries.frequencyPerSourceDataSet(args)
    frequencyPerSourceDataSet.show(frequencyPerSourceDataSet.count.toInt)

    val frequencyPerDayDataSet = sparkSqlQueries.frequencyPerDayDataSet(args)
    frequencyPerDayDataSet.show(frequencyPerDayDataSet.count.toInt)

    val aggregatedFrequencyPerDayDataSet = sparkSqlQueries.aggregatedFrequencyPerDayDataSet(args)
    aggregatedFrequencyPerDayDataSet.show(aggregatedFrequencyPerDayDataSet.count.toInt)

    spark.stop()
  }
}
