package spark

import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import io.findify.s3mock.S3Mock
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, TestSuite}

trait SparkS3TestWrapper extends TestSuite with BeforeAndAfter {

  val S3Host = "http://localhost:8001"
  val S3Region = "eu-west-2"
  var spark: SparkSession = _
  var api: S3Mock = _
  var client: AmazonS3 = _

  before {
    api = S3Mock(port = 8001, dir = "/tmp/s3")
    api.start

    spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    hadoopConf.set("fs.s3.impl", classOf[S3AFileSystem].getName)
    hadoopConf.set("fs.s3a.endpoint", S3Host)
    hadoopConf.set("fs.s3a.access.key", "accessKey")
    hadoopConf.set("fs.s3a.secret.key", "secretKey")

    hadoopConf.set("fs.s3a.attempts.maximum", "1")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.multiobjectdelete.enable", "false")
    hadoopConf.set("fs.s3a.change.detection.version.required", "false")
    hadoopConf.set("fs.s3a.connection.establish.timeout", "20000")
    hadoopConf.set("fs.s3a.connection.timeout", "20000")

    client = AmazonS3ClientBuilder.standard
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(new EndpointConfiguration(S3Host, S3Region))
      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
      .build
  }

  after {
    spark.stop()
    api.shutdown
  }
}
