package helper

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties


case class JdbcConnection(dbPath: String, connectionProperties: Properties) {
  private val connectionUrl = "jdbc:h2:" + dbPath + "/rss_news_articles"


  def getTableAsDataframe(tableName: String, spark: SparkSession): DataFrame = {
    spark.read.jdbc(connectionUrl, tableName, connectionProperties)
  }


  def createTableTempView(tableName: String, spark: SparkSession): Unit = {
    getTableAsDataframe(tableName, spark).createOrReplaceTempView(tableName)
  }
}
