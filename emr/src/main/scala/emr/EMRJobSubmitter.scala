package emr

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.elasticmapreduce.model._

import scala.collection.JavaConverters._
import scala.sys.env

object EMRJobSubmitter extends App {

  val clusterId = envOrThrow("CLUSTER_ID")
  val region = env.getOrElse("AWS_EMR_REGION", envOrThrow("AWS_DEFAULT_REGION"))

  val jobName = env.getOrElse("SPARK_JOB_NAME", System.currentTimeMillis().toString).trim
  val jobJarTargetPath = envOrThrow("SPARK_JOB_S3_PATH")
  val jobMainClass = envOrThrow("SPARK_JOB_CLASSPATH")
  val jobArguments = envOrThrow("SPARK_JOB_ARGUMENTS").split(" ").toList

  val emrClient = AmazonElasticMapReduceClientBuilder
    .standard()
    .withCredentials(new DefaultAWSCredentialsProviderChain())
    .withRegion(region)
    .build()

  val emrStepConfig = new StepConfig()
    .withName(jobName.trim)
    .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT)
    .withHadoopJarStep(
      new HadoopJarStepConfig()
        .withJar(jobJarTargetPath.trim)
        .withMainClass(jobMainClass)
        .withArgs(jobArguments.asJava)
    )

  val addStepRequest = new AddJobFlowStepsRequest()
    .withJobFlowId(clusterId)
    .withSteps(emrStepConfig)

  val addStepResult = emrClient.addJobFlowSteps(addStepRequest)
  val stepId = addStepResult.getStepIds.get(0)

  // Wait for the Spark job to complete
  val describeStepRequest = new DescribeStepRequest()
    .withClusterId(clusterId)
    .withStepId(stepId)

  var stepState = ""

  while (!stepState.equals("COMPLETED") && !stepState.equals("FAILED")) {
    val describeStepResult = emrClient.describeStep(describeStepRequest)
    stepState = describeStepResult.getStep.getStatus.getState
    Thread.sleep(5000)
  }

  // Get the final step state
  val describeStepResult = emrClient.describeStep(describeStepRequest)
  stepState = describeStepResult.getStep.getStatus.getState
  println(s"Spark job finished with state: $stepState")

  def envOrThrow(key: String) =
    sys.env.getOrElse(key, throw new IllegalStateException(s"environment var $key not provided"))

}
