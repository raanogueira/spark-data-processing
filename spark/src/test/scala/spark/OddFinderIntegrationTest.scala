package spark

import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.junit.JUnitRunner
import spark.OddFinder.TargetPath

@RunWith(classOf[JUnitRunner])
class OddFinderIntegrationTest extends AnyWordSpec with Matchers with SparkS3TestWrapper {

  "OddFinder" should {
    "read CSV and TSV files correctly from S3" in {
      val currentTime = System.currentTimeMillis()
      val bucket = s"test-run-$currentTime"
      val inputTestKey = s"input-$currentTime"

      client.createBucket(bucket)
      client.putObject(bucket, s"$inputTestKey/input1.csv", "col1,col2\n1,2\n1,3\n1,3\n")
      client.putObject(bucket, s"$inputTestKey/input2.tsv", "col3\tcol5\n9\t5\n9\t6\n9\t6\n")

      val df = OddFinder.read(spark)(TargetPath(s"s3a://$bucket/$inputTestKey"))

      df.count() should be(6)
    }
  }
}
