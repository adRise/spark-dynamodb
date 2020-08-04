package com.audienceproject.spark.dynamodb.datasource

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.audienceproject.shaded.google.common.util.concurrent.RateLimiter
import com.audienceproject.spark.dynamodb.connector.{
  ColumnSchema,
  TableConnector
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}

import scala.collection.mutable.ArrayBuffer

class DynamoMixedBatchWriter(
    batchSize: Int,
    columnSchema: ColumnSchema,
    opTypeColumn: ColumnSchema.Attr,
    connector: TableConnector,
    client: DynamoDB
) extends DataWriter[InternalRow] {

  protected val buffer: ArrayBuffer[InternalRow] =
    new ArrayBuffer[InternalRow](batchSize)
  protected val rateLimiter: RateLimiter =
    RateLimiter.create(connector.writeLimit)

  override def write(record: InternalRow): Unit = {
    buffer += record.copy()
    if (buffer.size == batchSize) {
      flush()
    }
  }

  override def commit(): WriterCommitMessage = {
    flush()
    new WriterCommitMessage {}
  }

  override def abort(): Unit = {}

  protected def flush(): Unit = {
    if (buffer.nonEmpty) {
      connector.putAndDeleteItems(columnSchema, opTypeColumn, buffer)(client, rateLimiter)
      buffer.clear()
    }
  }

}
