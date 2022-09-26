package spark.jdbc.persistence

import helper.MainHelper
import persistence.Extraction.ArticleExtractor
import spark.jdbc.persistence.DAO.ArticleDao


object SparkJdbcPersistence {
  def main(args: Array[String]): Unit = {
    /*
    TODO
      - args:
        - 0: newsFolderPath
        - 1: db path
      - add aggregate table
      - add readme
    */

    MainHelper.argsCheck(args, 2)
    val spark = MainHelper.createSparkSession
    val articleDao = ArticleDao(spark, args(1))

    ArticleExtractor(args(0))
      .foreach(article => {
        articleDao.save(article)
      })

    spark.stop()
  }
}
