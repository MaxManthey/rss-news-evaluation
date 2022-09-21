package helper

import org.scalatest.funsuite.AnyFunSuite
import wrapper.SparkSessionTestWrapper


class MainHelperTest extends AnyFunSuite with SparkSessionTestWrapper{
  test("argsCheck does not terminate") {
    MainHelper.argsCheck(Array("test", "test2", "test3"), 3)
  }


  test("argsCheck with wrong amount") {
    val test = MainHelper.createSparkSession
    assert(test == spark)
    test.stop()
    spark.stop()
  }
}
