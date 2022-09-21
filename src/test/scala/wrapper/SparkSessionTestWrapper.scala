package wrapper

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  private val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
}
