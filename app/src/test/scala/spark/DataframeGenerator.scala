package spark

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalacheck.Gen

object DataframeGenerator {

  val ColumnNames: Seq[String] = Seq("key", "value")

  def gen(spark: SparkSession)(config: Config = Config(3, 10)): Gen[DataFrame] = {
    val keyGen: Gen[Int] = Gen.choose(1, config.numberOfEntries)
    val oddNumGen: Gen[Int] = Gen.choose(1, config.numberOfEntries).map(_ * 2 + 1)
    val evenNumGen: Gen[Int] = Gen.choose(2, config.numberOfEntries).map(_ * 2)

    for {
      keys <- Gen.listOfN(config.numberOfKeys, keyGen)
      evenNumbers <- Gen.listOfN(10, evenNumGen)
      oddNum <- oddNumGen
    } yield {
      val data = keys.flatMap { key =>
        List.fill(oddNum)((key, oddNum)) ++
          evenNumbers.flatMap(evenNumber => List.fill(evenNumber)(key, evenNumber))
      }

      val schema = StructType(ColumnNames.map(name => StructField(name, IntegerType)))
      val rows = data.map(Row.fromTuple)
      spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    }
  }

  def config: Gen[Config] = {
    for {
      num1 <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
      num2 <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    } yield Config(num1, num2)
  }

  case class Config(numberOfKeys: Int, numberOfEntries: Int)
}
