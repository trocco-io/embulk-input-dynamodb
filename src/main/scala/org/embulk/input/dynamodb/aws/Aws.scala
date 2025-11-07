package org.embulk.input.dynamodb.aws

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder
import org.embulk.util.config.{Task => EmbulkTask}

object Aws {

  trait Task
      extends EmbulkTask
      with AwsCredentials.Task
      with AwsEndpointConfiguration.Task
      with AwsClientConfiguration.Task
      with AwsDynamodbConfiguration.Task

  def apply(task: Task): Aws = {
    new Aws(task)
  }

}

class Aws(task: Aws.Task) {

  def withDynamodb[A](f: DynamoDbClient => A): A = {
    val builder: DynamoDbClientBuilder = DynamoDbClient.builder()

    AwsEndpointConfiguration(task).configureDynamoDbClientBuilder(builder)
    AwsClientConfiguration(task).configureDynamoDbClientBuilder(builder)
    AwsDynamodbConfiguration(task).configureDynamoDbClientBuilder(builder)
    builder.credentialsProvider(
      AwsCredentials(task).createAwsCredentialsProvider
    )

    val svc = builder.build()
    try f(svc)
    finally svc.close()
  }

}
