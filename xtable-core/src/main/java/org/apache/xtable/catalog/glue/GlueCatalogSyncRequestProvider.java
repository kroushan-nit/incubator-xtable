/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.catalog.glue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

abstract class GlueCatalogSyncRequestProvider {

  @Getter private final GlueSchemaExtractor schemaExtractor;
  @Getter private final Configuration configuration;
  @Getter private final String tableFormat;

  protected static final String PROP_SPARK_SQL_SOURCES_PROVIDER = "spark.sql.sources.provider";
  protected static final String PROP_PATH = "path";
  protected static final String PROP_SERIALIZATION_FORMAT = "serialization.format";
  protected static final String PROP_EXTERNAL = "EXTERNAL";

  GlueCatalogSyncRequestProvider(
      Configuration configuration, GlueSchemaExtractor schemaExtractor, String tableFormat) {
    this.configuration = configuration;
    this.schemaExtractor = schemaExtractor;
    this.tableFormat = tableFormat;
  }

  abstract TableInput getCreateTableInput(
      InternalTable table, CatalogTableIdentifier tableIdentifier);

  abstract TableInput getUpdateTableInput(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier);

  static GlueCatalogSyncRequestProvider getInstance(
      String tableFormat, Configuration configuration, GlueSchemaExtractor schemaExtractor) {
    switch (tableFormat) {
      case TableFormat.ICEBERG:
        return new IcebergGlueCatalogSyncRequestProvider(configuration, schemaExtractor);
      case TableFormat.DELTA:
        return new DeltaGlueCatalogSyncRequestProvider(configuration, schemaExtractor);
      default:
        throw new NotSupportedException("Unsupported table format: " + tableFormat);
    }
  }

  @VisibleForTesting
  static List<Column> getNonPartitionColumns(InternalTable table, Map<String, Column> columnsMap) {
    List<String> partitionKeys = getPartitionKeys(table);
    return columnsMap.values().stream()
        .filter(c -> !partitionKeys.contains(c.name()))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  static List<Column> getPartitionColumns(InternalTable table, Map<String, Column> columnsMap) {
    return getPartitionKeys(table).stream()
        .map(
            fieldName ->
                Column.builder().name(fieldName).type(columnsMap.get(fieldName).type()).build())
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  static List<String> getPartitionKeys(InternalTable table) {
    List<String> partitionKeys = new ArrayList<>();
    table
        .getPartitioningFields()
        .forEach(
            field -> {
              if (!field.getPartitionFieldNames().isEmpty()) {
                partitionKeys.addAll(field.getPartitionFieldNames());
              } else {
                partitionKeys.add(field.getSourceField().getName());
              }
            });
    return partitionKeys;
  }
}
