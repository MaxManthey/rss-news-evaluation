package spark.jdbc.persistence.DAO

import helper.JdbcConnection
import org.apache.spark.sql.{SaveMode, SparkSession}
import persistence.DbClasses.AggregatedWordFrequency
import java.util.Properties


case class AggregatedWordFrequencyDao(spark: SparkSession, connectionUrl: String) {
  private val tableName = "AGGREGATED_WORD_FREQUENCY"
  private val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def saveIfNotExists(aggregatedWordFrequency: AggregatedWordFrequency): Unit =
    if(findId(aggregatedWordFrequency).isEmpty) save(aggregatedWordFrequency)


  def save(aggregatedWordFrequency: AggregatedWordFrequency): Unit = {
    try {
      val aggregatedWordFrequencyDF = spark.createDataFrame(Seq((aggregatedWordFrequency.frequency,
        aggregatedWordFrequency.newsWordId,
        aggregatedWordFrequency.date)))
        .toDF("frequency", "NEWS_WORD_ID", "date")

      aggregatedWordFrequencyDF.write.mode(SaveMode.Append).jdbc(connectionUrl, tableName, connectionProperties)
    } catch {
      case e: Exception => println(s"Error trying to add word: ${aggregatedWordFrequency.toString} ${e.getCause}")
        e.printStackTrace()
    }
  }


  def findId(aggregatedWordFrequency: AggregatedWordFrequency): Option[Int] = {
    try {
      val aggregatedWordFrequencyDF = JdbcConnection(connectionUrl, connectionProperties)
        .getTableAsDataframe(tableName, spark)
      val queryResult = aggregatedWordFrequencyDF.select("id")
        .where(aggregatedWordFrequencyDF("frequency") === aggregatedWordFrequency.frequency &&
          aggregatedWordFrequencyDF("news_word_id") === aggregatedWordFrequency.newsWordId &&
          aggregatedWordFrequencyDF("date") === aggregatedWordFrequency.date)
      if(queryResult.count.toInt > 0) return Some(queryResult.collect.toList.map(el=>el(0).toString.toInt).head)
    } catch {
      case e: Exception => println(s"Error trying to find word: ${aggregatedWordFrequency.toString} ${e.getCause}")
    }
    None
  }
}
