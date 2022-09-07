package helper

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object MainHelper {
  def argsCheck(args: Array[String], requiredAmount: Int): Unit = {
    if (args.length != requiredAmount) {
      println("Amount of provided args incorrect.")
      sys.exit(1)
    }
  }


  def createSparkSession: SparkSession = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
