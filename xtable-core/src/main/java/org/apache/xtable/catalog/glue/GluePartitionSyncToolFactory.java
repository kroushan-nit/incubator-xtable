package org.apache.xtable.catalog.glue;

import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.hadoop.conf.Configuration;
import org.apache.xtable.catalog.CatalogPartitionSyncOperations;
import org.apache.xtable.catalog.PartitionSyncTool;
import org.apache.xtable.hudi.HudiPartitionSyncTool;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.reflection.ReflectionUtils;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Optional;

public class GluePartitionSyncToolFactory {

  public static Optional<PartitionSyncTool> getPartitionSyncTool(
      String tableFormat, GlueCatalogConfig glueCatalogConfig, GlueClient glueClient, Configuration configuration) {

    switch (tableFormat) {
      case TableFormat.HUDI:
        String partitionValueExtractorClass = glueCatalogConfig.getPartitionExtractorClass();
        PartitionValueExtractor partitionValueExtractor = ReflectionUtils.createInstanceOfClass(partitionValueExtractorClass);
        CatalogPartitionSyncOperations glueCatalogPartitionSyncOperations = new GlueCatalogPartitionSyncOperations(glueClient, glueCatalogConfig);
        return Optional.of(new HudiPartitionSyncTool(glueCatalogPartitionSyncOperations, partitionValueExtractor, configuration));
      default:
        return Optional.empty();
    }
  }
}
