package spark

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import spark.OddFinder.TargetPath

object OddFinderRunner extends LazyLogging {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      logger.error(
        "Usage: OddFinderRunner <inputPath (local or s3)> <output path (local or s3)> <AWS profile name or empty to use the default provider chain>")
      System.exit(1)
    }

    val inputPath = TargetPath(args(0))
    val outputPath = TargetPath(args(1))
    val awsProfileName = args.lift(2)

    // Create SparkSession
    val sparkBuilder = SparkSession
      .builder()
      .master("local")
      .appName("OddFinderRunner")

    val spark = awsProfileName match {
      case Some(profileName) =>
        System.setProperty("aws.profile", profileName)
        sparkBuilder
          .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                  "com.amazonaws.auth.profile.ProfileCredentialsProvider")
          .getOrCreate()

      case None =>
        sparkBuilder
          .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                  classOf[DefaultAWSCredentialsProviderChain].getName)
          .getOrCreate()
    }

    val csv = OddFinder.read(spark)(inputPath)
    val output = OddFinder.find(csv)
    OddFinder.writeTsv(output, outputPath)

    // Stop SparkSession
    spark.stop()
  }
}
