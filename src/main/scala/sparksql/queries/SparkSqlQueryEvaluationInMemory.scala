package sparksql.queries

import helper.{JdbcConnection, MainHelper}
import persistence.DAO.ArticleDao
import persistence.DbClasses.{Article, DbConnectionFactory}
import persistence.Extraction.ArticleExtractor

import java.util.Properties


object SparkSqlQueryEvaluationInMemory {
  val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def main(args: Array[String]): Unit = {
    MainHelper.argsCheck(args, 4)

    //create tables with values in In-Memory DB
    val connectionFactory = DbConnectionFactory("jdbc:h2:mem:default")
    val articleDao = ArticleDao(connectionFactory)

    ArticleExtractor(args(0)).foreach {
      article: Article => articleDao.save(article)
    }

    articleDao.preAggregateSources()
    articleDao.closePrepared()

    //show results from In-Memory DB
    val spark = MainHelper.createSparkSession

    val jdbcConnection = JdbcConnection("jdbc:h2:mem:default", connectionProperties)
    val sourceDate = jdbcConnection.getTableAsDataframe("source_date", spark)
    val newsWord = jdbcConnection.getTableAsDataframe("news_word", spark)
    val wordFrequency = jdbcConnection.getTableAsDataframe("word_frequency", spark)
    val aggregatedWordFrequency = jdbcConnection.getTableAsDataframe("aggregated_word_frequency", spark)

    val sparkSqlQueries = SparkSqlQueries(sourceDate, newsWord, wordFrequency, aggregatedWordFrequency)

    val timer = System.nanoTime
    val aggregatedFrequencyPerDayDataSet = sparkSqlQueries.aggregatedFrequencyPerDayDataSet(args)
    val duration = (System.nanoTime - timer) / 1e9d

    aggregatedFrequencyPerDayDataSet.show(aggregatedFrequencyPerDayDataSet.count.toInt)
    println(duration + " Sec")

    connectionFactory.close()
    spark.stop()

  }
}