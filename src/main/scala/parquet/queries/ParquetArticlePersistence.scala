package parquet.queries

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import persistence.Extraction.ArticleExtractor
import java.sql.Date


case class ParquetArticlePersistence(spark: SparkSession, parquetFolderPath: String) {
  private val parquetPath =
    if(parquetFolderPath.charAt(parquetFolderPath.length-1) == '/') parquetFolderPath + "news-articles.parquet"
    else parquetFolderPath + "/news-articles.parquet"
  private val tmpParquetPath =
    if(parquetFolderPath.charAt(parquetFolderPath.length-1) == '/') parquetFolderPath + "news-articles.parquet"
    else parquetFolderPath + "/tmp-news-articles.parquet"

  private val parquetPersistenceHelper = ParquetPersistenceHelper(spark)


  def persistSourcesAsParquet(newsFolderPath: String): Unit = {
    val articleSchema = StructType(Array(
      StructField("word", StringType),
      StructField("frequency", IntegerType),
      StructField("source", StringType),
      StructField("date", DateType),
    ))
    val newArticleDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], articleSchema)
    parquetPersistenceHelper.createParquetFile(newArticleDF, tmpParquetPath)

    ArticleExtractor(newsFolderPath)
      .foreach(article => {
        val articleSeq = article.wordsMap.keys.map(word =>
          (word, article.wordsMap(word), article.source, Date.valueOf(article.date))).toSeq
        val currentArticleDF = spark.createDataFrame(articleSeq).toDF("word", "frequency", "source", "date")
        parquetPersistenceHelper.appendToParquetFile(currentArticleDF, tmpParquetPath)
      })

    val articleDF = parquetPersistenceHelper.readParquetFileAsDF(tmpParquetPath)
    parquetPersistenceHelper.createParquetFile(articleDF, parquetPath: String)

    parquetPersistenceHelper.createParquetFile(newArticleDF, tmpParquetPath)
  }


  def readParquetFileAsDF(): DataFrame =
    parquetPersistenceHelper.readParquetFileAsDF(parquetPath: String)
}
