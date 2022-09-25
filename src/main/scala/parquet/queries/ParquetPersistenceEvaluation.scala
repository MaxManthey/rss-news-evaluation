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

    val parquetArticleAggregation = ParquetArticleAggregation(spark, args(1))
    parquetArticleAggregation.aggregateArticlesAsParquet(articleDF)
    val aggregatedArticlesDF = parquetArticleAggregation.readAggregatedParquetFileAsDF()
    showSpecifiedQueryResult(aggregatedArticlesDF, args)

    spark.stop()
  }


  def showSpecifiedQueryResult(df: DataFrame, args: Array[String]): Unit = {
    val queryResult = df.select("*")
      .where(df("word") === args(2).toLowerCase && df("date") >= args(3) && df("date") <= args(4))
      .orderBy("date")
    queryResult.show(queryResult.count.toInt)
  }
}
