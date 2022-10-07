package parquet.queries

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import persistence.Extraction.ArticleExtractor
import java.sql.Date


case class ParquetArticlePersistence(spark: SparkSession, parquetFolderPath: String) {
  private val parquetPath =
    if(parquetFolderPath.charAt(parquetFolderPath.length-1) == '/') parquetFolderPath + "news-articles.parquet"
    else parquetFolderPath + "/news-articles.parquet"

  private val parquetPersistenceHelper = ParquetPersistenceHelper(spark)


  def persistSourcesAsParquet(newsFolderPath: String): Unit = {
    val articles = ArticleExtractor(newsFolderPath).flatMap(article =>
      article.wordsMap.keys.map(word =>
        (word, article.wordsMap(word), article.source, Date.valueOf(article.date))
      ).toSeq
    ).toSeq

    import spark.implicits._
    val articleDF = spark.sparkContext.parallelize(articles).toDF("word", "frequency", "source", "date")

    articleDF.write.mode(SaveMode.Overwrite).parquet(parquetPath)
  }


  def readParquetFileAsDF(): DataFrame =
    parquetPersistenceHelper.readParquetFileAsDF(parquetPath: String)
}
