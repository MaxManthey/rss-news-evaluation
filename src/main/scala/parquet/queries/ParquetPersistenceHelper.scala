package parquet.queries

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


case class ParquetPersistenceHelper(spark: SparkSession) {
  def createParquetFile(articleDF: DataFrame, parquetPath: String): Unit =
    articleDF.write.mode(SaveMode.Overwrite).parquet(parquetPath)


  def appendToParquetFile(articleDF: DataFrame, parquetPath: String): Unit =
  articleDF.write.mode(SaveMode.Append).parquet(parquetPath)


  def readParquetFileAsDF(parquetPath: String): DataFrame =
    spark.read.parquet(parquetPath)
}
