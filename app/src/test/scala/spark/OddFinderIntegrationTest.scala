package spark

import com.amazonaws.services.s3.model.{GetObjectRequest, S3Object}
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.junit.JUnitRunner
import spark.OddFinder.TargetPath

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

@RunWith(classOf[JUnitRunner])
class OddFinderIntegrationTest extends AnyWordSpec with Matchers with SparkS3TestWrapper {

  "OddFinder" should {
    "read CSV file correctly" in {
      val bucket = "test-run"
      val inputTestKey = "input/*.csv"
      val outputTestKey = s"output2"

      client.createBucket(bucket)
      client.putObject(bucket, "input/test1.csv", "col1,col2\n1,2\n1,3\n1,3\n")
      client.putObject(bucket, "input/test2.csv", "col3,col5\n9,5\n9,6\n9,6\n")

      val df = OddFinder.readDataFromS3(spark, TargetPath(s"s3a://$bucket/$inputTestKey").path)

      val output = OddFinder.find(df)

      client.listObjects(bucket).getObjectSummaries.forEach(println)

      OddFinder.writeTsv(output, TargetPath(s"s3a://$bucket/$outputTestKey"))

//      client.listObjects(inputTestKey).getObjectSummaries.forEach(println)
//
//      val s3Object: S3Object =
//        client.getObject(new GetObjectRequest(bucket, outputTestKey))
//      val s3ObjectContent: BufferedReader =
//        new BufferedReader(new InputStreamReader(s3Object.getObjectContent, StandardCharsets.UTF_8))
//
//      // Print the contents of the S3 object
//      s3ObjectContent.lines().forEach(println)
//
//      // Close the S3 object content and S3 client
//      s3ObjectContent.close()
    }
  }
}
