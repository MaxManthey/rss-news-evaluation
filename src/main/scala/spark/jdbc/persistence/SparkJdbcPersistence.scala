package spark.jdbc.persistence

import helper.MainHelper
import persistence.Extraction.ArticleExtractor
import spark.jdbc.persistence.DAO.{ArticleDao, PreAggregateArticleDao}


object SparkJdbcPersistence {
  def main(args: Array[String]): Unit = {
    MainHelper.argsCheck(args, 2)
    val spark = MainHelper.createSparkSession

//    val articleDao = ArticleDao(spark, args(1))
//
//    ArticleExtractor(args(0))
//      .foreach(article => {
//        articleDao.save(article)
//      })

    val timer = System.nanoTime

    val preAggregateArticleDao = PreAggregateArticleDao(spark, args(1))
    preAggregateArticleDao.preAggregate()

    val duration = (System.nanoTime - timer) / 1e9d
    println(duration + " Sec")

    spark.stop()

  }
}
