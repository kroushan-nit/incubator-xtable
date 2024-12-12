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
 
package org.apache.xtable.glue;

import java.time.ZonedDateTime;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.conversion.TargetCatalog;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.spi.sync.CatalogSyncClient;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;

@Log4j2
public abstract class GlueCatalogSyncClient implements CatalogSyncClient<Table> {

  protected static final String GLUE_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
  private static final String TEMP_SUFFIX = "_temp";

  protected final TargetCatalog targetCatalog;
  protected final GlueClient glueClient;
  protected final GlueCatalogConfig glueCatalogConfig;
  protected final Configuration configuration;
  protected final GlueSchemaExtractor schemaExtractor;

  public GlueCatalogSyncClient(TargetCatalog targetCatalog, Configuration configuration) {
    this.targetCatalog = targetCatalog;
    this.glueCatalogConfig =
        GlueCatalogConfig.createConfig(targetCatalog.getCatalogConfig().getCatalogOptions());
    this.glueClient = new DefaultGlueClientFactory(glueCatalogConfig).getGlueClient();
    this.configuration = new Configuration(configuration);
    this.schemaExtractor = GlueSchemaExtractor.getInstance();
  }

  GlueCatalogSyncClient(
      TargetCatalog targetCatalog,
      Configuration configuration,
      GlueCatalogConfig glueCatalogConfig,
      GlueClient glueClient,
      GlueSchemaExtractor schemaExtractor) {
    this.targetCatalog = targetCatalog;
    this.configuration = new Configuration(configuration);
    this.glueCatalogConfig = glueCatalogConfig;
    this.glueClient = glueClient;
    this.schemaExtractor = schemaExtractor;
  }

  @Override
  public String getCatalogId() {
    return targetCatalog.getCatalogId();
  }

  @Override
  public CatalogTableIdentifier getTableIdentifier() {
    return targetCatalog.getCatalogTableIdentifier();
  }

  @Override
  public String getStorageDescriptorLocation(Table table) {
    if (table == null || table.storageDescriptor() == null) {
      return null;
    }
    return table.storageDescriptor().location();
  }

  @Override
  public boolean hasDatabase(String databaseName) {
    try {
      return glueClient
              .getDatabase(
                  GetDatabaseRequest.builder()
                      .catalogId(glueCatalogConfig.getCatalogId())
                      .name(databaseName)
                      .build())
              .database()
          != null;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to get database: " + databaseName, e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    try {
      glueClient.createDatabase(
          CreateDatabaseRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseInput(
                  DatabaseInput.builder()
                      .name(databaseName)
                      .description("Created by " + this.getClass().getName())
                      .build())
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to create database: " + databaseName, e);
    }
  }

  @Override
  public Table getTable(CatalogTableIdentifier tableIdentifier) {
    try {
      GetTableResponse response =
          glueClient.getTable(
              GetTableRequest.builder()
                  .catalogId(glueCatalogConfig.getCatalogId())
                  .databaseName(tableIdentifier.getDatabaseName())
                  .name(tableIdentifier.getTableName())
                  .build());
      return response.table();
    } catch (EntityNotFoundException e) {
      return null;
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to get table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void createOrReplaceTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    // validate before dropping the table
    validateTempTableCreation(table, tableIdentifier);
    dropTable(table, tableIdentifier);
    createTable(table, tableIdentifier);
  }

  @Override
  public void dropTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    try {
      glueClient.deleteTable(
          DeleteTableRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseName(tableIdentifier.getDatabaseName())
              .name(tableIdentifier.getTableName())
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to drop table: " + tableIdentifier.getId(), e);
    }
  }

  /**
   * creates a temp table with new metadata and properties to ensure table creation succeeds before
   * dropping the table and recreating it. This ensures that actual table is not dropped in case
   * there are any issues
   */
  private void validateTempTableCreation(
      InternalTable table, CatalogTableIdentifier tableIdentifier) {
    String tempTableName =
        tableIdentifier.getTableName() + TEMP_SUFFIX + ZonedDateTime.now().toEpochSecond();
    CatalogTableIdentifier tempTableIdentifier =
        CatalogTableIdentifier.builder()
            .tableName(tempTableName)
            .databaseName(tableIdentifier.getDatabaseName())
            .build();
    createTable(table, tempTableIdentifier);
    dropTable(table, tempTableIdentifier);
  }

  @Override
  public void close() throws Exception {
    if (glueClient != null) {
      glueClient.close();
    }
  }
}
