package parquet.queries

import helper.MainHelper
import org.apache.spark.sql.DataFrame


object ParquetPersistenceEvaluation {
  def main(args: Array[String]): Unit = {
    MainHelper.argsCheck(args, 5)
    val spark = MainHelper.createSparkSession

    val parquetArticlePersistence = ParquetArticlePersistence(spark, args(1))
    parquetArticlePersistence.persistSourcesAsParquet(args(0))
    val articleDF = parquetArticlePersistence.readParquetFileAsDF()
    showSpecifiedQueryResult(articleDF, args)

    val aggPersistenceTimer = System.nanoTime

    val allArticlesDF = parquetArticlePersistence.readParquetFileAsDF()
    val parquetArticleAggregation = ParquetArticleAggregation(spark, args(1))
    parquetArticleAggregation.aggregateArticlesAsParquet(allArticlesDF)

    val aggPersistenceDuration = (System.nanoTime - aggPersistenceTimer) / 1e9d
    println(aggPersistenceDuration + " Sec")

    val aggregatedArticlesDF = parquetArticleAggregation.readAggregatedParquetFileAsDF()
    val aggQueryTimer = System.nanoTime
    val aggQueryResult = getQueryResult(aggregatedArticlesDF, args)
    val aggQueryDuration = (System.nanoTime - aggQueryTimer) / 1e9d
    println(aggQueryDuration + " Sec")
    aggQueryResult.show()

    spark.stop()
  }


  def showSpecifiedQueryResult(df: DataFrame, args: Array[String]): Unit = {
    val queryResult = getQueryResult(df, args)
    queryResult.show(queryResult.count.toInt)
  }

  def getQueryResult(df: DataFrame, args: Array[String]): DataFrame =
    df.select("*")
      .where(df("word") === args(2).toLowerCase && df("date") >= args(3) && df("date") <= args(4))
      .orderBy("date")
}
