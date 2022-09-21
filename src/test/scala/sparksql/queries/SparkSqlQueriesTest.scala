package sparksql.queries

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import wrapper.SparkSessionTestWrapper
import java.sql.Date


class SparkSqlQueriesTest extends AnyFunSuite with SparkSessionTestWrapper with BeforeAndAfter {
  private val argsInput = Array("pathToDB", "ukraine", "2022-05-16", "2022-10-16")

  private val wordFrequency = spark.createDataFrame(Seq((1, 2, 1, 1), (2, 3, 1, 2), (3, 4, 2, 1)))
    .toDF("id", "frequency", "news_word_id", "source_date_id")
  private val newsWord = spark.createDataFrame(Seq((1, "ukraine"), (2, "scholz")))
    .toDF("id", "word")
  private val sourceDate = spark.createDataFrame(Seq(
    (1, Date.valueOf("2022-07-16"), "www.test.de", "test"),
    (1, Date.valueOf("2022-07-17"), "www.test.com", "test2")
  )).toDF("id", "date", "source", "hashed_source")
  private val aggregatedWordFrequency = spark.createDataFrame(Seq(
    (1, 2, 1, Date.valueOf("2022-07-16")),
    (2, 4, 1, Date.valueOf("2022-07-17"))
  )).toDF("id", "frequency", "news_word_id", "date")
  private val sparkSqlQueries = SparkSqlQueries(sourceDate, newsWord, wordFrequency, aggregatedWordFrequency)


  test("frequencyPerSourceDataSet returns correct result") {
    val expectedResult = spark.createDataFrame(Seq(
      ("ukraine", 2, Date.valueOf("2022-07-16"), "www.test.de"),
      ("ukraine", 2, Date.valueOf("2022-07-17"), "www.test.com")
    )).toDF("word", "frequency", "date", "source")

    val queryResult = sparkSqlQueries.frequencyPerSourceDataSet(argsInput)

    assert(queryResult.collect sameElements expectedResult.collect)
  }


  test("frequencyPerDayDataSet returns correct result") {
    val expectedResult = spark.createDataFrame(Seq(
      (Date.valueOf("2022-07-16"), "ukraine", 2),
      (Date.valueOf("2022-07-17"), "ukraine", 2),
    )).toDF("date", "word", "frequency")

    val queryResult = sparkSqlQueries.frequencyPerDayDataSet(argsInput)

    assert(queryResult.collect sameElements expectedResult.collect)
  }


  test("aggregatedFrequencyPerDayDataSet returns correct result") {
    val expectedResult = spark.createDataFrame(Seq(
      ("ukraine", 2, Date.valueOf("2022-07-16")),
      ("ukraine", 4, Date.valueOf("2022-07-17")),
    )).toDF("word", "frequency", "date")

    val queryResult = sparkSqlQueries.aggregatedFrequencyPerDayDataSet(argsInput)

    assert(queryResult.collect sameElements expectedResult.collect)
  }
}
