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
 
package org.apache.xtable.glue.table;

import static org.apache.xtable.glue.GlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.glue.GlueCatalogSyncTestBase;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

@ExtendWith(MockitoExtension.class)
public class TestDeltaGlueCatalogTableBuilder extends GlueCatalogSyncTestBase {

  private DeltaGlueCatalogTableBuilder deltaGlueCatalogTableBuilder;

  private DeltaGlueCatalogTableBuilder createDeltaGlueCatalogSyncHelper() {
    return new DeltaGlueCatalogTableBuilder(mockGlueSchemaExtractor);
  }

  void setupCommonMocks() {
    deltaGlueCatalogTableBuilder = createDeltaGlueCatalogSyncHelper();
  }

  @Test
  void testGetCreateTableRequest() {
    setupCommonMocks();
    when(mockGlueSchemaExtractor.toColumns(
            TableFormat.DELTA, TEST_DELTA_INTERNAL_TABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());

    TableInput expected =
        TableInput.builder()
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .tableType(GLUE_EXTERNAL_TABLE_TYPE)
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(
                StorageDescriptor.builder()
                    .columns(Collections.emptyList())
                    .location(TEST_DELTA_INTERNAL_TABLE.getBasePath())
                    .serdeInfo(
                        SerDeInfo.builder()
                            .parameters(
                                deltaGlueCatalogTableBuilder.getSerDeParameters(
                                    TEST_DELTA_INTERNAL_TABLE))
                            .build())
                    .build())
            .partitionKeys(Collections.emptyList())
            .build();

    TableInput output =
        deltaGlueCatalogTableBuilder.getCreateTableRequest(
            TEST_DELTA_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    assertEquals(expected, output);
    verify(mockGlueSchemaExtractor, times(1))
        .toColumns(TableFormat.DELTA, TEST_DELTA_INTERNAL_TABLE.getReadSchema());
  }

  @Test
  void testGetUpdateTableInput() {
    setupCommonMocks();
    Table glueTable =
        Table.builder()
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(
                StorageDescriptor.builder()
                    .location(TEST_DELTA_INTERNAL_TABLE.getBasePath())
                    .serdeInfo(
                        SerDeInfo.builder()
                            .parameters(
                                deltaGlueCatalogTableBuilder.getSerDeParameters(
                                    TEST_DELTA_INTERNAL_TABLE))
                            .build())
                    .build())
            .build();
    when(mockGlueSchemaExtractor.toColumns(
            TableFormat.DELTA, TEST_DELTA_INTERNAL_TABLE.getReadSchema(), glueTable))
        .thenReturn(Collections.emptyList());

    TableInput expected =
        TableInput.builder()
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .tableType(GLUE_EXTERNAL_TABLE_TYPE)
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(
                StorageDescriptor.builder()
                    .columns(Collections.emptyList())
                    .location(TEST_DELTA_INTERNAL_TABLE.getBasePath())
                    .serdeInfo(
                        SerDeInfo.builder()
                            .parameters(
                                deltaGlueCatalogTableBuilder.getSerDeParameters(
                                    TEST_DELTA_INTERNAL_TABLE))
                            .build())
                    .build())
            .partitionKeys(Collections.emptyList())
            .build();

    TableInput output =
        deltaGlueCatalogTableBuilder.getUpdateTableRequest(
            TEST_DELTA_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);
    assertEquals(expected, output);
    verify(mockGlueSchemaExtractor, times(1))
        .toColumns(TableFormat.DELTA, TEST_DELTA_INTERNAL_TABLE.getReadSchema(), glueTable);
  }
}