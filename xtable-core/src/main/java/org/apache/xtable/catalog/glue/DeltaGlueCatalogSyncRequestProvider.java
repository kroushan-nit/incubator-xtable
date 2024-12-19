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

import static org.apache.xtable.catalog.glue.GlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

/** Delta specific table operations for Glue catalog sync */
class DeltaGlueCatalogSyncRequestProvider extends GlueCatalogSyncRequestProvider {

  DeltaGlueCatalogSyncRequestProvider(
      Configuration configuration, GlueSchemaExtractor schemaExtractor) {
    super(configuration, schemaExtractor, TableFormat.DELTA);
  }

  @Override
  TableInput getCreateTableInput(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(getTableParameters())
        .storageDescriptor(
            StorageDescriptor.builder()
                .columns(getSchemaWithoutPartitionKeys(table))
                .location(table.getBasePath())
                .serdeInfo(SerDeInfo.builder().parameters(getSerDeParameters(table)).build())
                .build())
        .partitionKeys(getPartitionKeys(table.getPartitioningFields()))
        .build();
  }

  @Override
  TableInput getUpdateTableInput(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier) {
    Map<String, String> parameters = new HashMap<>(catalogTable.parameters());
    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(parameters)
        .storageDescriptor(
            catalogTable.storageDescriptor().toBuilder()
                .columns(getSchemaWithoutPartitionKeys(table))
                .build())
        .partitionKeys(getPartitionKeys(table.getPartitioningFields()))
        .build();
  }

  @VisibleForTesting
  Map<String, String> getTableParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("table_type", "delta");
    parameters.put("spark.sql.sources.provider", "delta");
    parameters.put("EXTERNAL", "TRUE");
    return parameters;
  }

  @VisibleForTesting
  Map<String, String> getSerDeParameters(InternalTable table) {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("serialization.format", "1");
    parameters.put("path", table.getBasePath());
    return parameters;
  }
}
