package spark

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalacheck.Gen

object DataframeGenerator {

  val ColumnNames: Seq[String] = Seq("key", "value")

  def gen(): Unit = {
    val keyGen: Gen[Int] = Gen.choose(1, 10) // Generate keys between 1 and 100 (inclusive)
    val valueGen: Gen[List[Int]] = Gen.nonEmptyListOf(Gen.choose(1, 10 + 1))

    //Map with unique keys and a list of random numbers for each key
    val pairsGen: Gen[Map[Int, List[Int]]] = for {
      keys <- Gen.listOfN(2, keyGen)
      values <- Gen.listOfN(10, valueGen)
    } yield {
      keys.zip(values)
    }.toMap

    println(pairsGen.sample)

    val testingDataSet: Gen[Iterable[List[(Int, Int)]]] = pairsGen.map { entry =>
      entry.map {
        case (key, values) =>
          val valuesByNumberOfAppearances = values.groupBy(identity).mapValues(_.size)

          println(key, valuesByNumberOfAppearances, values)

          val uniqueOddNumberOfTimes = valuesByNumberOfAppearances
            .collectFirst {
              case (value, numberOfOccurrences) if numberOfOccurrences % 2 == 1 =>
                List.fill(numberOfOccurrences)((key, value))
            }
            .getOrElse(
              throw new IllegalStateException("Unable to find a number that " +
                s"appears an odd number of times in the generated dataset: $valuesByNumberOfAppearances")
            )

          val evenNumberOfTimes = valuesByNumberOfAppearances.collect {
            case (value, numberOfOccurrences) if numberOfOccurrences % 2 == 0 =>
              List.fill(numberOfOccurrences)((key, value))
          }.flatten

          uniqueOddNumberOfTimes ++ evenNumberOfTimes.toList
      }
    }
  }

  def gen(spark: SparkSession)(config: Config): Gen[DataFrame] = {
    val keyGen: Gen[Int] = Gen.choose(1, 10) // Generate keys between 1 and 100 (inclusive)
    val oddNumGen
      : Gen[Int] = Gen.choose(1, 10).map(_ * 2 + 1) // Generate odd numbers between 3 and 2001 (inclusive)
    val evenNumGen
      : Gen[Int] = Gen.choose(2, 10).map(_ * 2) // Generate even numbers between 4 and 2000 (inclusive)

    for {
      keys <- Gen.listOfN(3, keyGen) // Generate a list of 10 keys
      evenNumbers <- Gen.listOfN(10, evenNumGen) // Generate list of even numbers
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

  case class Config(numberOfEntries: Int, numberOfKeys: Int)
}
