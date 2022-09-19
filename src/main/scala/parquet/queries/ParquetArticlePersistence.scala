package parquet.queries

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import persistence.Extraction.ArticleExtractor
import java.sql.Date


case class ParquetArticlePersistence(spark: SparkSession, parquetFolderPath: String) {
  private val parquetPath =
    if(parquetFolderPath.charAt(parquetFolderPath.length-1) == '/') parquetFolderPath + "news-articles.parquet"
    else parquetFolderPath + "/news-articles.parquet"

  def persistSourcesAsParquet(fileDirName: String): Unit = {
    val articleDF = createArticleDF(fileDirName)
    parquetPersistence(articleDF)
  }


  private def createArticleDF(fileDirName: String): DataFrame = {
    val articleSchema = StructType(Array(
      StructField("word", StringType),
      StructField("frequency", IntegerType),
      StructField("source", StringType),
      StructField("date", DateType),
    ))
    var articleDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], articleSchema)

    ArticleExtractor(fileDirName)
      .foreach(article => {
        val moin = article.wordsMap.keys.map(word =>
          (word, article.wordsMap(word), article.source, Date.valueOf(article.date))).toSeq
        val singleArticleDF = spark.createDataFrame(moin).toDF("word", "frequency", "source", "date")
        articleDF = articleDF.union(singleArticleDF)
      })

    articleDF
  }


  private def parquetPersistence(articleDF: DataFrame): Unit =
    articleDF.write.mode(SaveMode.Overwrite).parquet(parquetPath)


  def readParquetFileAsDF(): DataFrame =
    spark.read.parquet(parquetPath)
}
