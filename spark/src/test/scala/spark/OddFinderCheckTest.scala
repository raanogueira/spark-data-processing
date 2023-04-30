package spark

import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class OddFinderCheckTest extends AnyWordSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  "OddsFinderCheck" should {
    "find odd occurrences for a simple case" in {
      val df = List(
        (1, 2),
        (1, 3),
        (1, 3)
      ).toDF("key", "value")

      val result = OddFinder.find(df)

      val expected = List((1, 2)).toDF("key", "value")

      result.except(expected).union(expected.except(result))
    }

    "find odd occurrences for a slightly more complex case" in {
      val df = List(
        (1, 10),
        (1, 10),
        (1, 10),
        (1, 3),
        (1, 3),
        (2, 4),
        (3, 5),
        (3, 5),
        (3, 5),
        (3, 5),
        (3, 6)
      ).toDF("key", "value")

      val result = OddFinder.find(df)

      val expected = List((1, 10), (2, 4), (3, 6)).toDF("key", "value")

      result.except(expected).union(expected.except(result))
    }

    "handle empty fields" in {
      val df = List(
        (1, None),
        (1, Some(3)),
        (1, Some(3))
      ).toDF("key", "value")

      val result = OddFinder.find(df)

      val expected = List((1, 0)).toDF("key", "value")

      result.except(expected).union(expected.except(result))
    }
  }
}
