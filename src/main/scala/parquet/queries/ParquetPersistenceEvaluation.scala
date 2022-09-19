package parquet.queries

import helper.MainHelper


object ParquetPersistenceEvaluation {
  def main(args: Array[String]): Unit = {
    MainHelper.argsCheck(args, 2)

    val spark = MainHelper.createSparkSession

    val parquetArticlePersistence = ParquetArticlePersistence(spark, args(1))

    parquetArticlePersistence.persistSourcesAsParquet(args(0))

    val articleDF = parquetArticlePersistence.readParquetFileAsDF()
    articleDF.show()

    spark.stop()
  }
}
