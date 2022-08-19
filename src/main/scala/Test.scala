import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    jsonTest(args, spark)

    spark.stop()
  }


  def jsonTest(args: Array[String], spark: SparkSession): Unit = {
    val jsonFile = args(0)
    val schema = StructType(
      Array(
        StructField("article", StringType),
        StructField("dateTime", StringType),
        StructField("source", StringType)
      )
    )
    val blogsDF = spark.read.option("multiline", "true").schema(schema).json(jsonFile)
    blogsDF.show(true)
    blogsDF.printSchema
  }
}
