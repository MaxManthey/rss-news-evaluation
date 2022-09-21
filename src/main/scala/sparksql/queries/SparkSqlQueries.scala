package sparksql.queries

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.sum


case class SparkSqlQueries(sourceDate: DataFrame,
                           newsWord: DataFrame,
                           wordFrequency: DataFrame,
                           aggregatedWordFrequency: DataFrame) {
  def basicQuery(args: Array[String]): Dataset[Row] =
    wordFrequency.as("wf")
      .join(newsWord.as("nw"), wordFrequency("news_word_id") ===  newsWord("id"),"full")
      .join(sourceDate.as("sd"), wordFrequency("source_date_id") ===  sourceDate("id"),"full")
      .where(newsWord("word") === args(1).toLowerCase && sourceDate("date") >= args(2) && sourceDate("date") <= args(3))


  def frequencyPerSourceDataSet(args: Array[String]): Dataset[Row] =
    basicQuery(args)
      .select("nw.word", "wf.frequency", "sd.date", "sd.source")
      .orderBy(sourceDate("date"))


  def frequencyPerDayDataSet(args: Array[String]): Dataset[Row] =
    basicQuery(args)
      .select("nw.word", "wf.frequency", "sd.date")
      .groupBy(sourceDate("date"), newsWord("word"))
      .agg(sum("wf.frequency"))
      .orderBy(sourceDate("date"))


  def aggregatedFrequencyPerDayDataSet(args: Array[String]): Dataset[Row] =
    aggregatedWordFrequency.as("awf")
      .join(newsWord.as("nw"), aggregatedWordFrequency("news_word_id") ===  newsWord("id"),"full")
      .select("nw.word", "awf.frequency", "awf.date")
      .where(newsWord("word") === args(1).toLowerCase && aggregatedWordFrequency("date") >= args(2) && aggregatedWordFrequency("date") <= args(3))
      .orderBy(aggregatedWordFrequency("date"))
}