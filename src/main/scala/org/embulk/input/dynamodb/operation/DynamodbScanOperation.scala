package org.embulk.input.dynamodb.operation

import java.util.Optional

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  ScanRequest
}
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import org.embulk.config.ConfigException
import org.embulk.util.config.{Config, ConfigDefault}
import org.embulk.input.dynamodb.logger

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.annotation.tailrec

object DynamodbScanOperation {

  trait Task extends AbstractDynamodbOperation.Task {

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
    @Config("segment")
    @ConfigDefault("null")
    def getSegment: Optional[Int]

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
    @Config("total_segment")
    @ConfigDefault("null")
    def getTotalSegment: Optional[Int]

  }
}

case class DynamodbScanOperation(task: DynamodbScanOperation.Task)
    extends AbstractDynamodbOperation(task) {

  override def getEmbulkTaskCount: Int = {
    if (task.getTotalSegment.isPresent && task.getSegment.isPresent) 1
    else if (task.getTotalSegment.isPresent && !task.getSegment.isPresent)
      task.getTotalSegment.get()
    else if (!task.getTotalSegment.isPresent && !task.getSegment.isPresent) 1
    else // if (!task.getTotalSegment.isPresent && task.getSegment.isPresent)
      throw new ConfigException(
        "\"segment\" option must be set with \"total_segment\" option."
      )
  }

  private def newRequest(
      embulkTaskIndex: Int,
      lastEvaluatedKey: Option[Map[String, AttributeValue]]
  ): ScanRequest = {
    val builder = ScanRequest.builder()
    configureRequest(builder, lastEvaluatedKey)

    task.getTotalSegment.ifPresent { totalSegment =>
      builder.totalSegments(totalSegment)
      builder.segment(task.getSegment.orElse(embulkTaskIndex))
    }

    builder.build()
  }

  @tailrec
  private def runInternal(
      dynamodb: DynamoDbClient,
      embulkTaskIndex: Int,
      f: Seq[Map[String, AttributeValue]] => Unit,
      lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
      loadedRecords: Long = 0
  ): Unit = {
    val loadableRecords: Option[Long] = calculateLoadableRecords(loadedRecords)

    val request = newRequest(embulkTaskIndex, lastEvaluatedKey)
    logger.info(s"Call DynamodbScanRequest: ${request.toString}")
    val result = dynamodb.scan(request)

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
            embulkTaskIndex,
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
  ): Unit = runInternal(dynamodb, embulkTaskIndex, f)
}
