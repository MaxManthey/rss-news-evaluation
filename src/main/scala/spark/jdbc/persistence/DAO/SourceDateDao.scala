package spark.jdbc.persistence.DAO

import helper.JdbcConnection
import org.apache.spark.sql.{SaveMode, SparkSession}
import persistence.DbClasses.SourceDate
import java.util.Properties


case class SourceDateDao(spark: SparkSession, connectionUrl: String) {
  private val tableName = "source_date"
  private val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")


  def saveIfNotExists(sourceDate: SourceDate): Unit = if(findId(sourceDate).isEmpty) save(sourceDate)


  def save(sourceDate: SourceDate): Unit = {
    try {
      val sourceDateDF = spark.createDataFrame(Seq((sourceDate.date, sourceDate.source, sourceDate.hashedSource)))
        .toDF("date", "source", "hashed_source")
      sourceDateDF.write.mode(SaveMode.Append).jdbc(connectionUrl, tableName, connectionProperties)
    } catch {
      case e: Exception => println(s"Error trying to add sourceDate: $sourceDate} ${e.getCause}")
    }
  }


  def findId(sourceDate: SourceDate): Option[Int] = {
    try {
      val sourceDateDF = JdbcConnection(connectionUrl, connectionProperties).getTableAsDataframe(tableName, spark)
      val queryResult = sourceDateDF.select("id")
        .where(sourceDateDF("source") === sourceDate.source &&
          sourceDateDF("hashed_source") === sourceDate.hashedSource &&
          sourceDateDF("date") === sourceDate.date)
      if(queryResult.count.toInt > 0) return Some(queryResult.collect.toList.map(el=>el(0).toString.toInt).head)
    } catch {
      case e: Exception => println(s"Error trying to find sourceDate: $sourceDate ${e.getCause}")
    }
    None
  }
}
