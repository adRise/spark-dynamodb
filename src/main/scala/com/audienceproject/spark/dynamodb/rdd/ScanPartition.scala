/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  *
  * Copyright © 2018 AudienceProject. All rights reserved.
  */
package com.audienceproject.spark.dynamodb.rdd

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity
import com.audienceproject.spark.dynamodb.connector.DynamoConnector
import org.apache.spark.Partition
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.spark_project.guava.util.concurrent.RateLimiter

import scala.collection.JavaConverters._

private[dynamodb] class ScanPartition(schema: StructType,
                                      partitionIndex: Int,
                                      connector: DynamoConnector)
    extends Partition with Serializable {

    @transient
    private lazy val aliasMap = schema.collect({
        case StructField(name, _, _, metadata) if metadata.contains("alias") =>
            name -> metadata.getString("alias")
    }).toMap

    @transient
    private lazy val typeConversions = schema.collect({
        case StructField(name, dataType, _, metadata) if metadata.contains("alias") =>
            name -> TypeConversion(metadata.getString("alias"), dataType)
        case StructField(name, dataType, _, _) =>
            name -> TypeConversion(name, dataType)
    }).toMap

    def scanTable(requiredColumns: Seq[String], filters: Seq[Filter]): Iterator[Row] = {

        val rateLimiter = RateLimiter.create(connector.rateLimit)

        val projectedColumns = requiredColumns.map(name => aliasMap.getOrElse(name, name))
        val scanResult = connector.scan(index, projectedColumns, filters)

        val pageIterator = scanResult.pages().iterator().asScala

        new Iterator[Row] {

            var innerIterator: Iterator[Row] = Iterator.empty
            var prevConsumedCapacity: Option[ConsumedCapacity] = None

            override def hasNext: Boolean = innerIterator.hasNext || pageIterator.hasNext

            override def next(): Row = {
                if (!innerIterator.hasNext) {
                    if (!pageIterator.hasNext) throw new NoSuchElementException("End of table")
                    else {
                        // Limit throughput to provisioned capacity.
                        prevConsumedCapacity
                            .map(capacity => Math.ceil(capacity.getCapacityUnits).toInt)
                            .foreach(rateLimiter.acquire)

                        val page = pageIterator.next()
                        prevConsumedCapacity = Option(page.getLowLevelResult.getScanResult.getConsumedCapacity)
                        innerIterator = page.getLowLevelResult.getItems.iterator().asScala.map(itemToRow(requiredColumns))
                    }
                }

                innerIterator.next()
            }

        }
    }

    private def itemToRow(requiredColumns: Seq[String])(item: Item): Row =
        if (requiredColumns.nonEmpty) Row.fromSeq(requiredColumns.map(columnName => typeConversions(columnName)(item)))
        else Row.fromSeq(item.asMap().asScala.values.toSeq.map(_.toString))

    override def index: Int = partitionIndex

}