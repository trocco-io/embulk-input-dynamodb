package org.embulk.input.dynamodb.aws

import java.util.Optional

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder
import software.amazon.awssdk.http.apache.ApacheHttpClient
import org.embulk.util.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.input.dynamodb.aws.AwsClientConfiguration.Task

object AwsClientConfiguration {

  trait Task extends EmbulkTask {

    @Config("http_proxy")
    @ConfigDefault("null")
    def getHttpProxy: Optional[HttpProxy.Task]

  }

  def apply(task: Task): AwsClientConfiguration = {
    new AwsClientConfiguration(task)
  }
}

class AwsClientConfiguration(task: Task) {

  def configureDynamoDbClientBuilder(
      builder: DynamoDbClientBuilder
  ): Unit = {
    task.getHttpProxy.ifPresent { v =>
      val proxyConfig = HttpProxy(v).configureProxyConfiguration().build()
      val httpClient = ApacheHttpClient
        .builder()
        .proxyConfiguration(proxyConfig)
        .build()
      builder.httpClient(httpClient)
    }
  }

}
