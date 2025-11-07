package org.embulk.input.dynamodb.item

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Optional, List => JList, Map => JMap}

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.core.SdkBytes
import org.embulk.util.config.{Config, ConfigDefault, Task => EmbulkTask}

import scala.jdk.CollectionConverters._
import scala.util.chaining._

/** TODO: I want to bind directly `org.embulk.util.config.Config`` to
  * `software.amazon.awssdk.services.dynamodb.model.AttributeValue`. Should I
  * implement a custom unmarshaller?
  */
object DynamodbAttributeValue {

  trait Task extends EmbulkTask {

    @Config("S")
    @ConfigDefault("null")
    def getS: Optional[String]

    @Config("N")
    @ConfigDefault("null")
    def getN: Optional[String]

    @Config("B")
    @ConfigDefault("null")
    def getB: Optional[String]

    @Config("SS")
    @ConfigDefault("null")
    def getSS: Optional[JList[String]]

    @Config("NS")
    @ConfigDefault("null")
    def getNS: Optional[JList[String]]

    @Config("BS")
    @ConfigDefault("null")
    def getBS: Optional[JList[String]]

    @Config("M")
    @ConfigDefault("null")
    def getM: Optional[JMap[String, DynamodbAttributeValue.Task]]

    @Config("L")
    @ConfigDefault("null")
    def getL: Optional[JList[DynamodbAttributeValue.Task]]

    @Config("NULL")
    @ConfigDefault("null")
    def getNULL: Optional[Boolean]

    @Config("BOOL")
    @ConfigDefault("null")
    def getBOOL: Optional[Boolean]
  }

  def apply(task: Task): DynamodbAttributeValue = {
    val builder = AttributeValue.builder()

    if (task.getS.isPresent) {
      builder.s(task.getS.get)
    }
    else if (task.getN.isPresent) {
      builder.n(task.getN.get)
    }
    else if (task.getB.isPresent) {
      builder.b(
        SdkBytes.fromByteArray(task.getB.get.getBytes(StandardCharsets.UTF_8))
      )
    }
    else if (task.getSS.isPresent) {
      builder.ss(task.getSS.get)
    }
    else if (task.getNS.isPresent) {
      builder.ns(task.getNS.get)
    }
    else if (task.getBS.isPresent) {
      builder.bs(
        task.getBS.get.asScala
          .map(e => SdkBytes.fromByteArray(e.getBytes(StandardCharsets.UTF_8)))
          .asJava
      )
    }
    else if (task.getM.isPresent) {
      builder.m(
        task.getM.get.asScala.map(x => (x._1, apply(x._2).getOriginal)).asJava
      )
    }
    else if (task.getL.isPresent) {
      builder.l(
        task.getL.get.asScala.map(apply).map(_.getOriginal).asJava
      )
    }
    else if (task.getNULL.isPresent) {
      builder.nul(task.getNULL.get)
    }
    else if (task.getBOOL.isPresent) {
      builder.bool(task.getBOOL.get)
    }

    new DynamodbAttributeValue(builder.build())
  }

  def apply(original: AttributeValue): DynamodbAttributeValue = {
    new DynamodbAttributeValue(original)
  }

  def apply(item: Map[String, AttributeValue]): DynamodbAttributeValue = {
    val original = AttributeValue.builder().m(item.asJava).build()
    new DynamodbAttributeValue(original)
  }
}

class DynamodbAttributeValue(original: AttributeValue) {

  require(
    message =
      s"Invalid AttributeValue: ${original} which must have 1 attribute value.",
    requirement = {
      Seq(hasS, hasN, hasB, hasSS, hasNS, hasBS, hasM, hasL, hasNULL, hasBOOL)
        .count(has => has) == 1
    }
  )

  def getOriginal: AttributeValue = original
  def isNull: Boolean = Option[Boolean](getOriginal.nul).getOrElse(false)
  def hasS: Boolean = getOriginal.s != null
  def hasN: Boolean = getOriginal.n != null
  def hasB: Boolean = getOriginal.b != null
  def hasSS: Boolean = getOriginal.hasSs
  def hasNS: Boolean = getOriginal.hasNs
  def hasBS: Boolean = getOriginal.hasBs
  def hasM: Boolean = getOriginal.hasM
  def hasL: Boolean = getOriginal.hasL
  def hasNULL: Boolean = getOriginal.nul != null
  def hasBOOL: Boolean = getOriginal.bool != null
  def getS: String = getOriginal.s
  def getN: String = getOriginal.n
  def getB: SdkBytes = getOriginal.b
  def getSS: JList[String] = getOriginal.ss
  def getNS: JList[String] = getOriginal.ns
  def getBS: JList[SdkBytes] = getOriginal.bs
  def getM: JMap[String, AttributeValue] = getOriginal.m
  def getL: JList[AttributeValue] = getOriginal.l
  def getNULL: Boolean = getOriginal.nul
  def getBOOL: Boolean = getOriginal.bool

  def getType: DynamodbAttributeValueType = {
    if (hasS) return DynamodbAttributeValueType.S
    if (hasN) return DynamodbAttributeValueType.N
    if (hasB) return DynamodbAttributeValueType.B
    if (hasSS) return DynamodbAttributeValueType.SS
    if (hasNS) return DynamodbAttributeValueType.NS
    if (hasBS) return DynamodbAttributeValueType.BS
    if (hasM) return DynamodbAttributeValueType.M
    if (hasL) return DynamodbAttributeValueType.L
    if (hasNULL) return DynamodbAttributeValueType.NULL
    if (hasBOOL) return DynamodbAttributeValueType.BOOL
    DynamodbAttributeValueType.UNKNOWN
  }
}
