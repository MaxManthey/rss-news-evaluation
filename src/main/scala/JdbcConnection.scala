import org.apache.spark.sql.SparkSession
import java.util.Properties


case class JdbcConnection(dbPath: String, connectionProperties: Properties) {
  private val connectionUrl = "jdbc:h2:" + dbPath + "/rss_news_articles"


  def createTableTempView(tableName: String, spark: SparkSession): Unit =
    spark.read.jdbc(connectionUrl, tableName, connectionProperties).createOrReplaceTempView(tableName)
}
