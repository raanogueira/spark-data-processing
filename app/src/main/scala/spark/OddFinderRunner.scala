package spark

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import spark.OddFinder.TargetPath

object OddFinderRunner {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: OddFinderRunner <inputPath (local or s3)> <output path (local or s3)>")
      System.exit(1)
    }

    val inputPath = TargetPath(args(0))
    val outputPath = TargetPath(args(1))

    // Create SparkSession
    val spark = SparkSession
      .builder()
      .appName("OddFinderRunner")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
              classOf[DefaultAWSCredentialsProviderChain].getName)
      .master("local")
      .getOrCreate()

    val csv = OddFinder.read(spark)(inputPath)
    val output = OddFinder.find(csv)
    OddFinder.writeTsv(output, outputPath)

    // Stop SparkSession
    spark.stop()
  }
}
