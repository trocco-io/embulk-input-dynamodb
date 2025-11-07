package org.embulk.input.dynamodb.operation

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  QueryRequest
}
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import org.embulk.util.config.{Config, ConfigDefault}
import org.embulk.input.dynamodb.logger

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.annotation.tailrec

object DynamodbQueryOperation {

  trait Task extends AbstractDynamodbOperation.Task {

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.KeyConditionExpressions
    @Config("key_condition_expression")
    def getKeyConditionExpression: String

    // TODO: Is it needed in the embulk context?
    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.KeyConditionExpressions
    @Config("scan_index_forward")
    @ConfigDefault("true")
    def getScanIndexForward: Boolean

  }
}

case class DynamodbQueryOperation(task: DynamodbQueryOperation.Task)
    extends AbstractDynamodbOperation(task) {

  private def newRequest(
      lastEvaluatedKey: Option[Map[String, AttributeValue]]
  ): QueryRequest = {
    val builder = QueryRequest.builder()
    configureRequest(builder, lastEvaluatedKey)
    builder.keyConditionExpression(task.getKeyConditionExpression)
    builder.scanIndexForward(task.getScanIndexForward)
    builder.build()
  }

  @tailrec
  private def runInternal(
      dynamodb: DynamoDbClient,
      f: Seq[Map[String, AttributeValue]] => Unit,
      lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
      loadedRecords: Long = 0
  ): Unit = {
    val loadableRecords: Option[Long] = calculateLoadableRecords(loadedRecords)

    val request = newRequest(lastEvaluatedKey)
    logger.info(s"Call DynamodbQueryRequest: ${request.toString}")
    val result = dynamodb.query(request)

    loadableRecords match {
      case Some(v) if (result.count() > v) =>
        f(result.items().asScala.take(v.toInt).map(_.asScala.toMap).toSeq)
      case _ =>
        f(result.items().asScala.map(_.asScala.toMap).toSeq)
        if (
          result.hasLastEvaluatedKey && result
            .lastEvaluatedKey() != null && !result.lastEvaluatedKey().isEmpty
        ) {
          runInternal(
            dynamodb,
            f,
            lastEvaluatedKey = Option(result.lastEvaluatedKey().asScala.toMap),
            loadedRecords = loadedRecords + result.count()
          )
        }
    }
  }

  override def run(
      dynamodb: DynamoDbClient,
      embulkTaskIndex: Int,
      f: Seq[Map[String, AttributeValue]] => Unit
  ): Unit = runInternal(dynamodb, f)
}
