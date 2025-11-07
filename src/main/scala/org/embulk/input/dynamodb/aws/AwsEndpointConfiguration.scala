package org.embulk.input.dynamodb.aws

import java.util.Optional
import java.net.URI

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import org.embulk.config.ConfigException
import org.embulk.util.config.{Task => EmbulkTask, Config, ConfigDefault}
import org.embulk.input.dynamodb.aws.AwsEndpointConfiguration.Task
import org.embulk.input.dynamodb.logger

import scala.util.Try

object AwsEndpointConfiguration {

  trait Task extends EmbulkTask {
    @Config("endpoint")
    @ConfigDefault("null")
    def getEndpoint: Optional[String]

    @Config("region")
    @ConfigDefault("null")
    def getRegion: Optional[String]
  }

  def apply(task: Task): AwsEndpointConfiguration = {
    new AwsEndpointConfiguration(task)
  }
}

class AwsEndpointConfiguration(task: Task) {

  def configureDynamoDbClientBuilder(
      builder: DynamoDbClientBuilder
  ): Unit = {
    if (task.getRegion.isPresent && task.getEndpoint.isPresent) {
      builder.region(Region.of(task.getRegion.get))
      builder.endpointOverride(URI.create(task.getEndpoint.get))
    }
    else if (task.getRegion.isPresent && !task.getEndpoint.isPresent) {
      builder.region(Region.of(task.getRegion.get))
    }
    else if (!task.getRegion.isPresent && task.getEndpoint.isPresent) {
      val r: Region = Try(new DefaultAwsRegionProviderChain().getRegion)
        .getOrElse(Region.US_EAST_1)
      builder.region(r)
      builder.endpointOverride(URI.create(task.getEndpoint.get))
    }
    else {
      // Use default region from provider chain
      val r: Region = Try(new DefaultAwsRegionProviderChain().getRegion)
        .getOrElse(Region.US_EAST_1)
      builder.region(r)
    }
  }

}
