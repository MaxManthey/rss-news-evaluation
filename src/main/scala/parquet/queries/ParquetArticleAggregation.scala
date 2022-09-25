package parquet.queries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


case class ParquetArticleAggregation(spark: SparkSession, parquetFolderPath: String) {
  private val aggregationFolderName = "/articles-aggregation.parquet"
  private val parquetPath =
    if(parquetFolderPath.charAt(parquetFolderPath.length-1) == '/') parquetFolderPath + aggregationFolderName
    else parquetFolderPath + aggregationFolderName

  private val parquetPersistenceHelper = ParquetPersistenceHelper(spark)


  def aggregateArticlesAsParquet(articlesDF: DataFrame): Unit = {
    val aggregatedArticles = articlesDF.select("*")
      .groupBy("date", "word")
      .agg(sum("frequency"))

    parquetPersistenceHelper.createParquetFile(aggregatedArticles, parquetPath: String)
  }

  def readAggregatedParquetFileAsDF(): DataFrame =
    parquetPersistenceHelper.readParquetFileAsDF(parquetPath: String)
}
