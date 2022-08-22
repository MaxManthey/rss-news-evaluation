import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties


object RssNewsEvaluation {
  val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "")
  private val frequencyPerSourceQuery = "SELECT NW.word, WF.frequency, SD.DATE, SD.SOURCE " +
    "FROM WORD_FREQUENCY WF " +
    "JOIN NEWS_WORD NW on WF.NEWS_WORD_ID = NW.ID " +
    "JOIN SOURCE_DATE SD on WF.SOURCE_DATE_ID = SD.ID " +
    "WHERE NW.word = 'ukraine' " +
    "AND SD.DATE BETWEEN '2022-05-20' AND '2022-08-21' " +
    "ORDER BY SD.DATE;"
  private val frequencyPerDayQuery = "SELECT NW.word, SUM(WF.frequency), SD.DATE " +
    "FROM WORD_FREQUENCY WF " +
    "JOIN NEWS_WORD NW on WF.NEWS_WORD_ID = NW.ID " +
    "JOIN SOURCE_DATE SD on WF.SOURCE_DATE_ID = SD.ID " +
    "WHERE NW.word = 'ukraine' " +
    "AND SD.DATE BETWEEN '2022-05-20' AND '2022-08-21' " +
    "GROUP BY SD.DATE, NW.word " +
    "ORDER BY SD.DATE;"


  def main(args: Array[String]): Unit = {
    if(args.length != 1) {
      println("Amount of provided args incorrect.")
      sys.exit(1)
    }

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val connectionUrl = "jdbc:h2:" + args(0) + "/rss_news_articles"
    val sourceDateDF = spark.read.jdbc(connectionUrl, "source_date", connectionProperties)
    sourceDateDF.createOrReplaceTempView("source_date")
    val newsWordDF = spark.read.jdbc(connectionUrl, "news_word", connectionProperties)
    newsWordDF.createOrReplaceTempView("news_word")
    val wordFrequencyDF = spark.read.jdbc(connectionUrl, "word_frequency", connectionProperties)
    wordFrequencyDF.createOrReplaceTempView("word_frequency")

    val frequencyPerSourceDF = spark.sql(frequencyPerSourceQuery)
    frequencyPerSourceDF.show(frequencyPerSourceDF.count.toInt)

    val frequencyPerDayDF = spark.sql(frequencyPerDayQuery)
    frequencyPerDayDF.show(frequencyPerDayDF.count.toInt)

    spark.stop()
  }
}
//TODO logs schreiben
//TODO in Klasse auslagern
