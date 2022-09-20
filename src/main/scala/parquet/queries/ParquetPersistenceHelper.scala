package parquet.queries

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


case class ParquetPersistenceHelper(spark: SparkSession, parquetPath: String) {
  def createParquetFile(articleDF: DataFrame): Unit =
    articleDF.write.mode(SaveMode.Overwrite).parquet(parquetPath)


  def appendToParquetFile(articleDF: DataFrame): Unit =
  articleDF.write.mode(SaveMode.Append).parquet(parquetPath)


  def readParquetFileAsDF(): DataFrame =
    spark.read.parquet(parquetPath)
}
