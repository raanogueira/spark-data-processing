package spark

import org.apache.spark.sql.functions.{coalesce, col, count, lit, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

object OddFinder {

  case class TargetPath(path: String) extends AnyVal

  def find(
    df: DataFrame
  ): DataFrame = {
    //random columns, so fetch them
    val columnNames = df.columns
    val keyColumnName = columnNames.headOption.getOrElse(
      throw new IllegalStateException("Unable to retrieve key (first) column name/header"))
    val valueColumnName = columnNames.lastOption.getOrElse(
      throw new IllegalStateException("Unable to retrieve value (second) column name/header"))

    df.show()

    df.withColumn(valueColumnName,
                  when(col(valueColumnName).isNull.or(col(valueColumnName) === ""), lit(0))
                    .otherwise(col(valueColumnName)))
      .groupBy(valueColumnName, keyColumnName)
      .agg(count(valueColumnName).alias("count"))
      .filter(col("count") % 2 === 1)
      .select(col(keyColumnName), col(valueColumnName))
  }

  def readCsv(
    spark: SparkSession
  )(
    filePath: TargetPath
  ): DataFrame = {

    def readFile(delimiter: String): Option[DataFrame] = {
      Try {
        spark.read
          .option("header", "true")
          .option("delimiter", delimiter)
          .option("inferSchema", "true")
          .csv(s"${filePath.path}")
      }.toOption
    }
    val csv = readFile(",")
    val tsv = readFile("\t")

    (tsv, csv) match {
      case (Some(t), Some(c)) => c.union(t)
      case (None, Some(c))    => c
      case (Some(t), None)    => t
      case (None, None)       => throw new IllegalStateException("Unable to find a CSV or TSV file")
    }
  }

  def read(
    spark: SparkSession
  )(
    path: TargetPath
  ): DataFrame = {

    import spark.implicits._

    // Read the file(s) or directory from S3
    val df: DataFrame = spark.read
      .format("csv") // Specify the default format as CSV
      .option("header", "true") // If the file has a header
      .option("delimiter", ",") // Set the default delimiter as comma (for CSV)
      .load(path.path)

    // If it's a single file, return the DataFrame directly
    if (df.count() <= 1)
      df
    else {
      // If it's a directory, read all files within the directory and combine into a single DataFrame
      val fileList: Array[String] = df.select("path").as[String].collect()
      val combinedDF: DataFrame = fileList.foldLeft(spark.emptyDataFrame)((df1, file) => {
        val format = file.endsWith(".tsv") match {
          case true  => "csv" // Treat .tsv files as CSV format
          case false => "csv" // Treat other files as CSV format by default
        }
        val delimiter = format match {
          case "csv" => "," // Set the delimiter as comma for CSV
          case "tsv" => "\t" // Set the delimiter as tab for TSV
        }
        val fileDF = spark.read
          .format(format)
          .option("header", "true")
          .option("delimiter", delimiter)
          .load(file)
        df1.union(fileDF)
      })
      combinedDF
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
