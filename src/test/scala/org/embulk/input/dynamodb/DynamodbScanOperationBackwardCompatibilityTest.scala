package org.embulk.input.dynamodb

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  CreateTableRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  PutItemRequest,
  ScalarAttributeType
}
import org.embulk.config.ConfigSource
import org.embulk.input.dynamodb.testutil.EmbulkTestBase
import org.hamcrest.CoreMatchers._
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import org.msgpack.value.Value

import scala.jdk.CollectionConverters._

class DynamodbScanOperationBackwardCompatibilityTest extends EmbulkTestBase {

  private def testBackwardCompatibility(embulkInConfig: ConfigSource): Unit = {
    cleanupTable("EMBULK_DYNAMODB_TEST_TABLE")
    withDynamodb { dynamodb =>
      dynamodb.createTable(
        CreateTableRequest
          .builder()
          .tableName("EMBULK_DYNAMODB_TEST_TABLE")
          .attributeDefinitions(
            AttributeDefinition
              .builder()
              .attributeName("pri-key")
              .attributeType(ScalarAttributeType.S)
              .build(),
            AttributeDefinition
              .builder()
              .attributeName("sort-key")
              .attributeType(ScalarAttributeType.N)
              .build()
          )
          .keySchema(
            KeySchemaElement
              .builder()
              .attributeName("pri-key")
              .keyType(KeyType.HASH)
              .build(),
            KeySchemaElement
              .builder()
              .attributeName("sort-key")
              .keyType(KeyType.RANGE)
              .build()
          )
          .provisionedThroughput(
            ProvisionedThroughput
              .builder()
              .readCapacityUnits(5L)
              .writeCapacityUnits(5L)
              .build()
          )
          .build()
      )

      dynamodb.putItem(
        PutItemRequest
          .builder()
          .tableName("EMBULK_DYNAMODB_TEST_TABLE")
          .item(
            Map(
              "pri-key" -> AttributeValue.builder().s("key-1").build(),
              "sort-key" -> AttributeValue.builder().n("0").build(),
              "doubleValue" -> AttributeValue.builder().n("42.195").build(),
              "boolValue" -> AttributeValue.builder().bool(true).build(),
              "listValue" -> AttributeValue
                .builder()
                .l(
                  AttributeValue.builder().s("list-value").build(),
                  AttributeValue.builder().n("123").build()
                )
                .build(),
              "mapValue" -> AttributeValue
                .builder()
                .m(
                  Map(
                    "map-key-1" -> AttributeValue
                      .builder()
                      .s("map-value-1")
                      .build(),
                    "map-key-2" -> AttributeValue.builder().n("456").build()
                  ).asJava
                )
                .build()
            ).asJava
          )
          .build()
      )
    }

    runInput(
      embulkInConfig,
      { result: Seq[Seq[AnyRef]] =>
        val head = result.head
        assertThat(head(0).toString, is("key-1"))
        assertThat(head(1).asInstanceOf[Long], is(0L))
        assertThat(head(2).asInstanceOf[Double], is(42.195))
        assertThat(head(3).asInstanceOf[Boolean], is(true))

        val arrayValue = head(4).asInstanceOf[Value].asArrayValue()
        assertThat(arrayValue.size(), is(2))
        assertThat(arrayValue.get(0).asStringValue().toString, is("list-value"))
        assertThat(arrayValue.get(1).asIntegerValue().asLong(), is(123L))

        val mapValue = head(5).asInstanceOf[Value].asMapValue()
        assert(mapValue.keySet().asScala.map(_.toString).contains("map-key-1"))
        assertThat(
          mapValue
            .entrySet()
            .asScala
            .filter(_.getKey.toString.equals("map-key-1"))
            .head
            .getValue
            .toString,
          is("map-value-1")
        )
        assert(mapValue.keySet().asScala.map(_.toString).contains("map-key-2"))
        assertThat(
          mapValue
            .entrySet()
            .asScala
            .filter(_.getKey.toString.equals("map-key-2"))
            .head
            .getValue
            .asIntegerValue()
            .asLong(),
          is(456L)
        )
      }
    )
  }

}
