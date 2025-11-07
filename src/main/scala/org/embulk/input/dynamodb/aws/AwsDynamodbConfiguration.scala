package org.embulk.input.dynamodb.aws

import java.util.Optional

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder
import org.embulk.util.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.input.dynamodb.aws.AwsDynamodbConfiguration.Task

object AwsDynamodbConfiguration {

  trait Task extends EmbulkTask {

    @Config("enable_endpoint_discovery")
    @ConfigDefault("null")
    def getEnableEndpointDiscovery: Optional[Boolean]

  }

  def apply(task: Task): AwsDynamodbConfiguration = {
    new AwsDynamodbConfiguration(task)
  }
}

class AwsDynamodbConfiguration(task: Task) {

  def configureDynamoDbClientBuilder(
      builder: DynamoDbClientBuilder
  ): Unit = {
    task.getEnableEndpointDiscovery.ifPresent { v =>
      builder.endpointDiscoveryEnabled(v)
    }
  }

}
