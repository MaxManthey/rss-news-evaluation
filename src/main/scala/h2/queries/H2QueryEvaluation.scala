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
    jdbcConnection.createTableTempView("aggregated_word_frequency", spark)

    val h2Queries = H2Queries(spark, args)

    h2Queries.frequencyPerSource()
    h2Queries.frequencyPerDay()
    h2Queries.frequencyPerDayPreAggregation()

    spark.stop()
  }
}
