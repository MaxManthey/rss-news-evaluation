package parquet.queries

import helper.MainHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import java.sql.Date
import persistence.Extraction.ArticleExtractor



object ParquetPersistenceEvaluation {
  def main(args: Array[String]): Unit = {
    MainHelper.argsCheck(args, 0)

    val spark = MainHelper.createSparkSession

    val articleSchema = StructType(Array(
      StructField("word", StringType),
      StructField("frequency", IntegerType),
      StructField("source", StringType),
      StructField("date", DateType),
    ))
    var articleDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], articleSchema)

    ArticleExtractor("/Users/max/Development/Thesis/all-news-files/test-news-files")
      .foreach(article => {
        val moin = article.wordsMap.keys.map(word =>
          (word, article.wordsMap(word), article.source, Date.valueOf(article.date))).toSeq
        val singleArticleDF = spark.createDataFrame(moin).toDF("word", "frequency", "source", "date")
        singleArticleDF.show()
        articleDF = articleDF.union(singleArticleDF)
      })

    val test = articleDF.select("word", "frequency")
    test.show()
    articleDF.show()

    spark.stop()
  }
}
