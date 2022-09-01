package sparksql.queries

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties
import h2.queries.JdbcConnection


object RssNewsEvaluation {
  val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Amount of provided args incorrect.")
      sys.exit(1)
    }

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //TODO replace with spark sql queries
    val jdbcConnection = JdbcConnection(args(0), connectionProperties)
    jdbcConnection.createTableTempView("source_date", spark)
    jdbcConnection.createTableTempView("news_word", spark)
    jdbcConnection.createTableTempView("word_frequency", spark)

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

    spark.stop()
  }
}
