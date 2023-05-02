package spark

import org.apache.spark.sql.functions.{col, count, lit, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

object OddFinder {

  case class TargetPath(path: String) extends AnyVal

  /**
    * Finds out which values occurred an odd number of time for each key.
    * A possible improvement would be using reduceByKey
    * instead of groupBy to reduce data transferred across partitions (less shuffling )
    */
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
      .repartition(col(keyColumnName))
      .groupBy(valueColumnName, keyColumnName)
      .agg(count(valueColumnName).alias("count"))
      .filter(col("count") % 2 === 1)
      .select(col(keyColumnName), col(valueColumnName))
  }

  /**
    * Reads all all CSV and TSV files within a directory or S3 bucket and merge them into one dataframe.
    * We could use parquet or a custom implementation to balance the number of files across all workers
    */
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
      case (None, None) =>
        throw new IllegalStateException(s"Unable to find a CSV or TSV file in ${filePath.path}")
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
