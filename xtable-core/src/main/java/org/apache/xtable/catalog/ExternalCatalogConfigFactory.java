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
 
package org.apache.xtable.catalog;

import java.util.Map;

import org.apache.xtable.catalog.glue.GlueCatalogConversionSource;
import org.apache.xtable.catalog.glue.GlueCatalogSyncClient;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.storage.CatalogType;

/** A factory class which returns {@link ExternalCatalogConfig} based on catalogType. */
public class ExternalCatalogConfigFactory {

  public static ExternalCatalogConfig fromCatalogType(
      String catalogType, String catalogId, Map<String, String> properties) {
    String catalogSyncClientImpl;
    String catalogConversionSourceImpl;
    switch (catalogType) {
      case CatalogType.GLUE:
        catalogSyncClientImpl = GlueCatalogSyncClient.class.getName();
        catalogConversionSourceImpl = GlueCatalogConversionSource.class.getName();
        break;
      default:
        throw new NotSupportedException("Unsupported catalogType: " + catalogType);
    }
    return ExternalCatalogConfig.builder()
        .catalogType(catalogType)
        .catalogSyncClientImpl(catalogSyncClientImpl)
        .catalogConversionSourceImpl(catalogConversionSourceImpl)
        .catalogId(catalogId)
        .catalogProperties(properties)
        .build();
  }
}
