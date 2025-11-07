package org.embulk.input.dynamodb

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  BillingMode,
  CreateTableRequest,
  KeySchemaElement,
  KeyType,
  PutItemRequest,
  ScalarAttributeType
}
import org.embulk.config.ConfigSource
import org.embulk.input.dynamodb.testutil.EmbulkTestBase
import org.junit.Test

import scala.jdk.CollectionConverters._

class DynamodbOperationTest extends EmbulkTestBase {

  private val runDynamodbOperationTest: Boolean = Option(
    System.getenv("RUN_DYNAMODB_OPERATION_TEST")
  ) match {
    case Some(x) =>
      if (x == "false") false
      else true
    case None => true
  }

  @Test
  def limitTest(): Unit = if (runDynamodbOperationTest) {
    val tableName = "limit_test"
    cleanupTable(tableName)
    withDynamodb { dynamodb =>
      dynamodb.createTable(
        CreateTableRequest
          .builder()
          .tableName(tableName)
          .attributeDefinitions(
            AttributeDefinition
              .builder()
              .attributeName("pk")
              .attributeType(ScalarAttributeType.S)
              .build()
          )
          .keySchema(
            KeySchemaElement
              .builder()
              .attributeName("pk")
              .keyType(KeyType.HASH)
              .build()
          )
          .billingMode(BillingMode.PAY_PER_REQUEST)
          .build()
      )
      dynamodb.putItem(
        PutItemRequest
          .builder()
          .tableName(tableName)
          .item(
            Map(
              "pk" -> AttributeValue.builder().s("a").build()
            ).asJava
          )
          .build()
      )
      dynamodb.putItem(
        PutItemRequest
          .builder()
          .tableName(tableName)
          .item(
            Map(
              "pk" -> AttributeValue.builder().s("a").build()
            ).asJava
          )
          .build()
      )
      dynamodb.putItem(
        PutItemRequest
          .builder()
          .tableName(tableName)
          .item(
            Map(
              "pk" -> AttributeValue.builder().s("a").build()
            ).asJava
          )
          .build()
      )
      dynamodb.putItem(
        PutItemRequest
          .builder()
          .tableName(tableName)
          .item(
            Map(
              "pk" -> AttributeValue.builder().s("b").build()
            ).asJava
          )
          .build()
      )
      dynamodb.putItem(
        PutItemRequest
          .builder()
          .tableName(tableName)
          .item(
            Map(
              "pk" -> AttributeValue.builder().s("b").build()
            ).asJava
          )
          .build()
      )
    }

    val inScanConfig: ConfigSource = loadConfigSourceFromYamlString(s"""
           |type: dynamodb
           |table: $tableName
           |endpoint: http://$dynamoDBHost:$dynamoDBPort/
           |auth_method: basic
           |access_key_id: dummy
           |secret_access_key: dummy
           |scan:
           |  limit: 1
           |""".stripMargin)
    runInput(
      inScanConfig,
      { result =>
        assert(result.size.equals(1))
      }
    )

    val inQueryConfig: ConfigSource = loadConfigSourceFromYamlString(s"""
             |type: dynamodb
             |table: $tableName
             |endpoint: http://$dynamoDBHost:$dynamoDBPort/
             |auth_method: basic
             |access_key_id: dummy
             |secret_access_key: dummy
             |query:
             |  key_condition_expression: "#x = :v"
             |  expression_attribute_names:
             |    "#x": pk
             |  expression_attribute_values:
             |    ":v": {S: a}
             |  limit: 1
             |""".stripMargin)
    runInput(
      inQueryConfig,
      { result =>
        assert(result.size.equals(1))
      }
    )
  }
}
