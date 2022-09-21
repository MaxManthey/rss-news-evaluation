package parquet.queries

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import persistence.Extraction.ArticleExtractor
import java.sql.Date


case class ParquetArticlePersistence(spark: SparkSession, parquetFolderPath: String) {
  private val parquetPath =
    if(parquetFolderPath.charAt(parquetFolderPath.length-1) == '/') parquetFolderPath + "news-articles.parquet"
    else parquetFolderPath + "/news-articles.parquet"

  private val parquetPersistenceHelper = ParquetPersistenceHelper(spark, parquetPath)


  def persistSourcesAsParquet(newsFolderPath: String): Unit = {
    val articleSchema = StructType(Array(
      StructField("word", StringType),
      StructField("frequency", IntegerType),
      StructField("source", StringType),
      StructField("date", DateType),
    ))
    val articleDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], articleSchema)
    parquetPersistenceHelper.createParquetFile(articleDF)

    ArticleExtractor(newsFolderPath)
      .foreach(article => {
        val articleSeq = article.wordsMap.keys.map(word =>
          (word, article.wordsMap(word), article.source, Date.valueOf(article.date))).toSeq
        val newArticleDF = spark.createDataFrame(articleSeq).toDF("word", "frequency", "source", "date")
        parquetPersistenceHelper.appendToParquetFile(newArticleDF)
      })
  }


  def readParquetFileAsDF(): DataFrame =
    parquetPersistenceHelper.readParquetFileAsDF()
}
