package spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties
import spark.DataframeGenerator.Config

object OddFinderCheckPropertyTest
    extends Properties("OddFinderCheck")
    with SparkSessionTestWrapper {

  property("find at least one key with an odd number of occurrences") =
    forAll(DataframeGenerator.gen(spark)(Config(2, 3))) { df: DataFrame =>
      val result = OddFinder.find(df)
      val total = result.count()

      total > 0 &&
      result.select(col("key")).distinct().count() == total &&
        result.filter(col("value") % 2 === 1).count() == total
    }
}
