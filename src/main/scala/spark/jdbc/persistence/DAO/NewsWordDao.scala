package spark.jdbc.persistence.DAO

import helper.JdbcConnection
import org.apache.spark.sql.{SaveMode, SparkSession}
import persistence.DbClasses.NewsWord

import java.util.Properties


case class NewsWordDao(spark: SparkSession, pathToDb: String) {
  private val tableName = "news_word"
  private val connectionUrl = "jdbc:h2:" + pathToDb + "/rss_news_articles"
  private val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def saveIfNotExists(newsWord: NewsWord): Unit = if(findId(newsWord).isEmpty) save(newsWord)


  def save(newsWord: NewsWord): Unit = {
    try {
      val newsWordDF = spark.createDataFrame(Seq(newsWord)).toDF("word")
      newsWordDF.write.mode(SaveMode.Append).jdbc(connectionUrl, tableName, connectionProperties)
    } catch {
      case e: Exception => println(s"Error trying to add word: ${newsWord.word} ${e.getCause}")
        e.printStackTrace()
    }
  }

  def findId(newsWord: NewsWord): Option[Int] = {
    try {
      val newsWordDF = JdbcConnection(pathToDb, connectionProperties).getTableAsDataframe(tableName, spark)
      val queryResult = newsWordDF.select("id")
        .where(newsWordDF("word") === newsWord.word)
      if(queryResult.count.toInt > 0) return Some(queryResult.collect.toList.map(el=>el(0).toString.toInt).head)
    } catch {
      case e: Exception => println(s"Error trying to find sourceDate: ${newsWord.word} ${e.getCause}")
    }
    None
  }
}
