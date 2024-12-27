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
 
package org.apache.xtable.catalog.hms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.catalog.CatalogPartitionSyncOperations;
import org.apache.xtable.catalog.Partition;
import org.apache.xtable.exception.CatalogSyncException;

@ExtendWith(MockitoExtension.class)
public class TestHMSCatalogPartitionSyncOperations extends HMSCatalogSyncTestBase {

  private CatalogPartitionSyncOperations mockHMSPartitionSyncOperations;

  void setupCommonMocks() {
    mockHMSPartitionSyncOperations =
        new HMSCatalogPartitionSyncOperations(mockMetaStoreClient, mockHMSCatalogConfig);
  }

  @SneakyThrows
  @Test
  void testGetAllPartitions() {
    setupCommonMocks();

    org.apache.hadoop.hive.metastore.api.Partition hivePartition1 =
        new org.apache.hadoop.hive.metastore.api.Partition();
    hivePartition1.setValues(Collections.singletonList("value1"));
    StorageDescriptor sd1 = new StorageDescriptor();
    sd1.setLocation("location1");
    hivePartition1.setSd(sd1);

    org.apache.hadoop.hive.metastore.api.Partition hivePartition2 =
        new org.apache.hadoop.hive.metastore.api.Partition();
    hivePartition2.setValues(Collections.singletonList("value2"));
    StorageDescriptor sd2 = new StorageDescriptor();
    sd2.setLocation("location2");
    hivePartition2.setSd(sd2);

    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions =
        Arrays.asList(hivePartition1, hivePartition2);
    when(mockMetaStoreClient.listPartitions(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName(),
            (short) -1))
        .thenReturn(hivePartitions);
    List<Partition> partitions =
        mockHMSPartitionSyncOperations.getAllPartitions(TEST_CATALOG_TABLE_IDENTIFIER);

    assertEquals(2, partitions.size());
    assertEquals("location1", partitions.get(0).getStorageLocation());
    assertEquals(1, partitions.get(0).getValues().size());
    assertEquals("value1", partitions.get(0).getValues().get(0));
    assertEquals("location2", partitions.get(1).getStorageLocation());
    assertEquals(1, partitions.get(1).getValues().size());
    assertEquals("value2", partitions.get(1).getValues().get(0));

    verify(mockMetaStoreClient, times(1))
        .listPartitions(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName(),
            (short) -1);
  }

  @Test
  void testAddPartitionsToTableSuccess() throws Exception {
    setupCommonMocks();
    when(mockHMSCatalogConfig.getMaxPartitionsPerRequest()).thenReturn(100);
    Partition partition1 = new Partition(Collections.singletonList("value1"), "location1");
    Partition partition2 = new Partition(Collections.singletonList("value2"), "location2");
    List<Partition> partitionsToAdd = Arrays.asList(partition1, partition2);

    StorageDescriptor tableSd = new StorageDescriptor();
    tableSd.setCols(Collections.emptyList());
    tableSd.setInputFormat("inputFormat");
    tableSd.setOutputFormat("outputFormat");
    tableSd.setSerdeInfo(new SerDeInfo());

    org.apache.hadoop.hive.metastore.api.Table table =
        new org.apache.hadoop.hive.metastore.api.Table();
    table.setSd(tableSd);

    when(mockMetaStoreClient.getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName()))
        .thenReturn(table);

    // Execute the method
    mockHMSPartitionSyncOperations.addPartitionsToTable(
        TEST_CATALOG_TABLE_IDENTIFIER, partitionsToAdd);

    // Verify behavior
    ArgumentCaptor<List<org.apache.hadoop.hive.metastore.api.Partition>> partitionCaptor =
        ArgumentCaptor.forClass(List.class);

    verify(mockMetaStoreClient, times(1))
        .add_partitions(partitionCaptor.capture(), eq(true), eq(false));

    // Validate the captured partitions
    List<org.apache.hadoop.hive.metastore.api.Partition> capturedPartitions =
        partitionCaptor.getValue();
    assertEquals(2, capturedPartitions.size());

    org.apache.hadoop.hive.metastore.api.Partition capturedPartition1 = capturedPartitions.get(0);
    assertEquals(partition1.getValues(), capturedPartition1.getValues());
    assertEquals(partition1.getStorageLocation(), capturedPartition1.getSd().getLocation());

    org.apache.hadoop.hive.metastore.api.Partition capturedPartition2 = capturedPartitions.get(1);
    assertEquals(partition2.getValues(), capturedPartition2.getValues());
    assertEquals(partition2.getStorageLocation(), capturedPartition2.getSd().getLocation());
  }

  @Test
  void testAddPartitionsToTableThrowsException() throws Exception {
    setupCommonMocks();
    List<Partition> partitionsToAdd =
        Collections.singletonList(new Partition(Collections.singletonList("value1"), "location1"));

    when(mockMetaStoreClient.getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName()))
        .thenThrow(new TException("Test exception"));

    // Execute and validate exception
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                mockHMSPartitionSyncOperations.addPartitionsToTable(
                    TEST_CATALOG_TABLE_IDENTIFIER, partitionsToAdd));

    assertInstanceOf(TException.class, exception.getCause());
    verify(mockMetaStoreClient, times(1))
        .getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
    verify(mockMetaStoreClient, never()).add_partitions(anyList(), anyBoolean(), anyBoolean());
  }

  @Test
  void testUpdatePartitionsToTableSuccess() throws Exception {
    setupCommonMocks();

    Partition changedPartition1 = new Partition(Collections.singletonList("value1"), "location1");
    Partition changedPartition2 = new Partition(Collections.singletonList("value2"), "location2");
    List<Partition> changedPartitions = Arrays.asList(changedPartition1, changedPartition2);

    StorageDescriptor tableSd = new StorageDescriptor();
    tableSd.setCols(Collections.emptyList());
    tableSd.setInputFormat("inputFormat");
    tableSd.setOutputFormat("outputFormat");
    tableSd.setSerdeInfo(new SerDeInfo());

    Table table = new Table();
    table.setSd(tableSd);

    when(mockMetaStoreClient.getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName()))
        .thenReturn(table);

    // Execute the method
    mockHMSPartitionSyncOperations.updatePartitionsToTable(
        TEST_CATALOG_TABLE_IDENTIFIER, changedPartitions);

    // Capture calls to dropPartition and add_partition
    ArgumentCaptor<List<String>> dropPartitionCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<org.apache.hadoop.hive.metastore.api.Partition> addPartitionCaptor =
        ArgumentCaptor.forClass(org.apache.hadoop.hive.metastore.api.Partition.class);

    verify(mockMetaStoreClient, times(2))
        .dropPartition(
            eq(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName()),
            eq(TEST_CATALOG_TABLE_IDENTIFIER.getTableName()),
            dropPartitionCaptor.capture(),
            eq(false));
    verify(mockMetaStoreClient, times(2)).add_partition(addPartitionCaptor.capture());

    // Validate the dropPartition calls
    List<List<String>> droppedPartitionValues = dropPartitionCaptor.getAllValues();
    assertEquals(changedPartition1.getValues(), droppedPartitionValues.get(0));
    assertEquals(changedPartition2.getValues(), droppedPartitionValues.get(1));

    // Validate the add_partition calls
    List<org.apache.hadoop.hive.metastore.api.Partition> addedPartitions =
        addPartitionCaptor.getAllValues();
    assertEquals(2, addedPartitions.size());

    org.apache.hadoop.hive.metastore.api.Partition addedPartition1 = addedPartitions.get(0);
    assertEquals(changedPartition1.getValues(), addedPartition1.getValues());
    assertEquals(changedPartition1.getStorageLocation(), addedPartition1.getSd().getLocation());

    org.apache.hadoop.hive.metastore.api.Partition addedPartition2 = addedPartitions.get(1);
    assertEquals(changedPartition2.getValues(), addedPartition2.getValues());
    assertEquals(changedPartition2.getStorageLocation(), addedPartition2.getSd().getLocation());
  }

  @Test
  void testUpdatePartitionsToTableThrowsException() throws Exception {
    setupCommonMocks();
    Partition changedPartition = new Partition(Collections.singletonList("value1"), "location1");

    when(mockMetaStoreClient.getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName()))
        .thenThrow(new TException("Test exception"));

    // Execute and validate exception
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                mockHMSPartitionSyncOperations.updatePartitionsToTable(
                    TEST_CATALOG_TABLE_IDENTIFIER, Collections.singletonList(changedPartition)));

    assertInstanceOf(TException.class, exception.getCause());

    verify(mockMetaStoreClient, times(1))
        .getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
    verify(mockMetaStoreClient, never())
        .dropPartition(anyString(), anyString(), anyList(), anyBoolean());
    verify(mockMetaStoreClient, never()).add_partition(any());
  }

  @Test
  void testDropPartitionsSuccess() throws Exception {
    setupCommonMocks();

    Partition partition1 = new Partition(Collections.singletonList("value1"), "location1");
    Partition partition2 = new Partition(Collections.singletonList("value2"), "location2");
    List<Partition> partitionsToDrop = Arrays.asList(partition1, partition2);

    // Execute the method
    mockHMSPartitionSyncOperations.dropPartitions(TEST_CATALOG_TABLE_IDENTIFIER, partitionsToDrop);

    // Capture calls to dropPartition
    ArgumentCaptor<List<String>> partitionValuesCaptor = ArgumentCaptor.forClass(List.class);

    verify(mockMetaStoreClient, times(2))
        .dropPartition(
            eq(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName()),
            eq(TEST_CATALOG_TABLE_IDENTIFIER.getTableName()),
            partitionValuesCaptor.capture(),
            eq(false));

    // Validate captured arguments
    List<List<String>> capturedPartitionValues = partitionValuesCaptor.getAllValues();
    assertEquals(2, capturedPartitionValues.size());
    assertEquals(partition1.getValues(), capturedPartitionValues.get(0));
    assertEquals(partition2.getValues(), capturedPartitionValues.get(1));
  }

  @Test
  void testDropPartitionsEmptyList() throws Exception {
    setupCommonMocks();
    List<Partition> partitionsToDrop = Collections.emptyList();

    mockHMSPartitionSyncOperations.dropPartitions(TEST_CATALOG_TABLE_IDENTIFIER, partitionsToDrop);

    // Verify no calls to dropPartition
    verify(mockMetaStoreClient, never())
        .dropPartition(anyString(), anyString(), anyList(), anyBoolean());
  }

  @Test
  void testDropPartitionsThrowsException() throws Exception {
    setupCommonMocks();

    Partition partition1 = new Partition(Collections.singletonList("value1"), "location1");
    List<Partition> partitionsToDrop = Collections.singletonList(partition1);

    doThrow(new TException("Test exception"))
        .when(mockMetaStoreClient)
        .dropPartition(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName(),
            partition1.getValues(),
            false);

    // Execute and validate exception
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                mockHMSPartitionSyncOperations.dropPartitions(
                    TEST_CATALOG_TABLE_IDENTIFIER, partitionsToDrop));

    assertInstanceOf(TException.class, exception.getCause());

    // Verify dropPartition call is made once
    verify(mockMetaStoreClient, times(1))
        .dropPartition(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName(),
            partition1.getValues(),
            false);
  }

  @Test
  void testGetTablePropertiesSuccess() throws Exception {
    setupCommonMocks();

    List<String> lastSyncedKeys = Arrays.asList("key1", "key2", "key3");

    Map<String, String> mockParameters = new HashMap<>();
    mockParameters.put("key1", "value1");
    mockParameters.put("key2", "value2");
    mockParameters.put("irrelevantKey", "irrelevantKey");

    Table mockTable = new Table();
    mockTable.setParameters(mockParameters);

    when(mockMetaStoreClient.getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName()))
        .thenReturn(mockTable);

    // Execute the method
    Map<String, String> result =
        mockHMSPartitionSyncOperations.getTableProperties(
            TEST_CATALOG_TABLE_IDENTIFIER, lastSyncedKeys);

    // Validate the result
    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
    assertNull(result.get("key3")); // key3 is not in mockParameters
  }

  @Test
  void testGetTablePropertiesThrowsException() throws Exception {
    setupCommonMocks();

    List<String> lastSyncedKeys = Arrays.asList("key1", "key2");

    when(mockMetaStoreClient.getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName()))
        .thenThrow(new TException("Test exception"));

    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                mockHMSPartitionSyncOperations.getTableProperties(
                    TEST_CATALOG_TABLE_IDENTIFIER, lastSyncedKeys));

    assertInstanceOf(TException.class, exception.getCause());
  }

  @Test
  void testUpdateTablePropertiesSuccess() throws Exception {
    setupCommonMocks();

    Map<String, String> lastTimeSyncedProperties = new HashMap<>();
    lastTimeSyncedProperties.put("last_synced_time", "2023-12-01T12:00:00Z");
    lastTimeSyncedProperties.put("last_modified_by", "user123");

    Map<String, String> existingParameters = new HashMap<>();
    existingParameters.put("existing_key", "existing_value");

    Table mockTable = new Table();
    mockTable.setParameters(existingParameters);

    when(mockMetaStoreClient.getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName()))
        .thenReturn(mockTable);

    // Execute the method
    mockHMSPartitionSyncOperations.updateTableProperties(
        TEST_CATALOG_TABLE_IDENTIFIER, lastTimeSyncedProperties);

    // Verify behavior
    verify(mockMetaStoreClient, times(1))
        .alter_table(
            eq(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName()),
            eq(TEST_CATALOG_TABLE_IDENTIFIER.getTableName()),
            eq(mockTable));

    // Validate updated parameters
    Map<String, String> updatedParameters = mockTable.getParameters();
    assertEquals(3, updatedParameters.size());
    assertEquals("2023-12-01T12:00:00Z", updatedParameters.get("last_synced_time"));
    assertEquals("user123", updatedParameters.get("last_modified_by"));
    assertEquals("existing_value", updatedParameters.get("existing_key"));
  }

  @Test
  void testUpdateTablePropertiesNoChanges() throws Exception {
    setupCommonMocks();

    // Empty properties map
    Map<String, String> lastTimeSyncedProperties = Collections.emptyMap();

    // Execute the method
    mockHMSPartitionSyncOperations.updateTableProperties(
        TEST_CATALOG_TABLE_IDENTIFIER, lastTimeSyncedProperties);

    // Verify no calls to MetaStoreClient
    verify(mockMetaStoreClient, never()).getTable(anyString(), anyString());
    verify(mockMetaStoreClient, never()).alter_table(anyString(), anyString(), any());
  }

  @Test
  void testUpdateTablePropertiesThrowsException() throws Exception {
    setupCommonMocks();

    Map<String, String> lastTimeSyncedProperties =
        Collections.singletonMap("last_synced_time", "2023-12-01T12:00:00Z");

    when(mockMetaStoreClient.getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName()))
        .thenThrow(new TException("Test exception"));

    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                mockHMSPartitionSyncOperations.updateTableProperties(
                    TEST_CATALOG_TABLE_IDENTIFIER, lastTimeSyncedProperties));

    assertInstanceOf(TException.class, exception.getCause());

    // Verify no alter table calls are made
    verify(mockMetaStoreClient, times(1))
        .getTable(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
    verify(mockMetaStoreClient, never()).alter_table(anyString(), anyString(), any());
  }
}