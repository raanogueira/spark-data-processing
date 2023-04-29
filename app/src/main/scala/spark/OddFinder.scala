package spark

import org.apache.spark.sql.functions.{coalesce, col, count, lit, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

object OddFinder {

  case class TargetPath(path: String) extends AnyVal

  def find(
    df: DataFrame
  ): DataFrame = {
    val columnNames = df.columns
    val keyColumnName = columnNames.headOption.getOrElse(
      throw new IllegalStateException("Unable to retrieve key (first) column name/header"))
    val valueColumnName = columnNames.lastOption.getOrElse(
      throw new IllegalStateException("Unable to retrieve value (second) column name/header"))

    df.withColumn(valueColumnName,
                  when(col(valueColumnName).isNull.or(col(valueColumnName) === ""), lit(0))
                    .otherwise(col(valueColumnName)))
      .repartition(df.rdd.getNumPartitions)
      .groupBy(valueColumnName, keyColumnName)
      .agg(count(valueColumnName).alias("count"))
      .filter(col("count") % 2 === 1)
      .select(col(keyColumnName), col(valueColumnName))
  }

  def read(
    spark: SparkSession
  )(
    filePath: TargetPath
  ): DataFrame = {

    def readFile(delimiter: String, extension: String): Option[DataFrame] = {
      Try {
        spark.read
          .option("header", "true")
          .option("delimiter", delimiter)
          .option("inferSchema", "true")
          .csv(s"${filePath.path}/$extension")
      }.toOption
    }
    val csv = readFile(",", "*.csv")
    val tsv = readFile("\t", "*.tsv")

    (tsv, csv) match {
      case (Some(t), Some(c)) => c.union(t)
      case (None, Some(c))    => c
      case (Some(t), None)    => t
      case (None, None)       => throw new IllegalStateException("Unable to find a CSV or TSV file")
    }
  }

  def writeTsv(
    df: DataFrame,
    path: TargetPath
  ): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(path.path)
  }
}
