package spark.jdbc.persistence.DAO

import org.apache.spark.sql.SparkSession
import persistence.DbClasses.{Article, DbConnectionFactory, NewsWord, SourceDate, WordFrequency}

import java.security.MessageDigest

case class ArticleDao(spark: SparkSession, connectionUrl: String) {
  //drop existing Tables and create required tables
  private val dbConnectionFactory = DbConnectionFactory(connectionUrl)
  dbConnectionFactory.close()

  private val sourceDateDao = SourceDateDao(spark, connectionUrl)
  private val newsWordDao = NewsWordDao(spark, connectionUrl)
  private val wordFrequencyDao = WordFrequencyDao(spark, connectionUrl)


  def save(article: Article): Unit = {
    try {
      val sourceDate = SourceDate(article.date, article.source,
        MessageDigest.getInstance("MD5").digest(article.source.getBytes).map("%02x".format(_)).mkString)
      sourceDateDao.saveIfNotExists(sourceDate)
      val sourceDateId = sourceDateDao.findId(sourceDate) match {
        case Some(value) => value
        case None => -1
      }

      for(word <- article.wordsMap.keys) {
        val newsWord = NewsWord(word)
        newsWordDao.saveIfNotExists(newsWord)
        val newsWordId = newsWordDao.findId(newsWord) match {
          case Some(value) => value
          case None => -1
        }
        wordFrequencyDao.saveIfNotExists(WordFrequency(article.wordsMap(word), newsWordId, sourceDateId))
      }
    } catch {
      case e: Exception => println(s"Error trying to save article: ${article.toString}" + e.getCause)
    }
  }
}
