package sparksql.queries

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import wrapper.SparkSessionTestWrapper
import java.sql.Date


class SparkSqlQueryEvaluationTest extends AnyFunSuite with SparkSessionTestWrapper with BeforeAndAfter {
  private val wordFrequency = spark.createDataFrame(Seq((1, 2, 1, 1), (2, 3, 1, 2), (3, 4, 2, 1)))
    .toDF("id", "frequency", "news_word_id", "source_date_id")
  private val newsWord = spark.createDataFrame(Seq((1, "ukraine"), (2, "scholz")))
    .toDF("id", "word")
  private val sourceDate = spark.createDataFrame(Seq((1, Date.valueOf("2022-07-16"), "www.test.de", "test"),
    (1, Date.valueOf("2022-07-17"), "www.test.com", "test2")))
    .toDF("id", "date", "source", "hashed_source")

  before {
    spark
  }
  after {
    spark.stop()
  }

  test("d") {

    val basicQuery = wordFrequency.as("wf")
      .join(newsWord.as("nw"), wordFrequency("news_word_id") ===  newsWord("id"),"full")
      .join(sourceDate.as("sd"), wordFrequency("source_date_id") ===  sourceDate("id"),"full")
      .where(newsWord("word") === "ukraine".toLowerCase && sourceDate("date") >= "2022-05-16" && sourceDate("date") <= "2022-09-16")

    basicQuery.show()
  }
}
