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

import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.xtable.catalog.CatalogPartitionSyncOperations;
import org.apache.xtable.catalog.PartitionSyncTool;
import org.apache.xtable.hudi.HudiPartitionSyncTool;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.reflection.ReflectionUtils;

import software.amazon.awssdk.services.glue.GlueClient;

public class GluePartitionSyncToolFactory {

  public static Optional<PartitionSyncTool> getPartitionSyncTool(
      String tableFormat,
      GlueCatalogConfig glueCatalogConfig,
      GlueClient glueClient,
      Configuration configuration) {

    switch (tableFormat) {
      case TableFormat.HUDI:
        String partitionValueExtractorClass = glueCatalogConfig.getPartitionExtractorClass();
        PartitionValueExtractor partitionValueExtractor =
            ReflectionUtils.createInstanceOfClass(partitionValueExtractorClass);
        CatalogPartitionSyncOperations glueCatalogPartitionSyncOperations =
            new GlueCatalogPartitionSyncOperations(glueClient, glueCatalogConfig);
        return Optional.of(
            new HudiPartitionSyncTool(
                glueCatalogPartitionSyncOperations, partitionValueExtractor, configuration));
      default:
        return Optional.empty();
    }
  }
}
