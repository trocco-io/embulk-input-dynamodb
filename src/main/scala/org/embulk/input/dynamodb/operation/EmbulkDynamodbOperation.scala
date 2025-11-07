package org.embulk.input.dynamodb.operation

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

trait EmbulkDynamodbOperation {

  def getEmbulkTaskCount: Int = 1

  def run(
      dynamodb: DynamoDbClient,
      embulkTaskIndex: Int,
      f: Seq[Map[String, AttributeValue]] => Unit
  ): Unit
}
