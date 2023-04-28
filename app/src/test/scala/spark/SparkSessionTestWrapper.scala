package spark

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .getOrCreate()
  }

  spark.sparkContext.setLogLevel("ERROR")
}
