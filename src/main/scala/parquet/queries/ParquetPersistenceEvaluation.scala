package parquet.queries

import helper.MainHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import persistence.Extraction.ArticleExtractor


object ParquetPersistenceEvaluation {
  def main(args: Array[String]): Unit = {
    MainHelper.argsCheck(args, 0)

    val spark = MainHelper.createSparkSession

    val newsWordSchema = StructType(Array(StructField("id", IntegerType), StructField("word", StringType)))
//    val sourceDateSchema = StructType(Array(
//      StructField("id", IntegerType),
//      StructField("date", DataType),
//      StructField("source", String),
//      StructField("hashed_source", StringType)
//    ))
//    spark.createDataFrame()
//    val test = spark.emptyDataFrame
    val newsWordDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], newsWordSchema)


//    ArticleExtractor("/Users/max/Development/Thesis/all-news-files/test-news-files").foreach{
//      println
//    }

    spark.stop()
  }
}
