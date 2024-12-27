package org.apache.xtable.catalog.glue.table;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConfigUtils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.catalog.glue.GlueCatalogConfig;
import org.apache.xtable.catalog.glue.GlueSchemaExtractor;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.hudi.HudiSparkDataSourceTableUtils;
import org.apache.xtable.hudi.HudiTableManager;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.TableFormat;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getInputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getOutputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getSerDeClassName;
import static org.apache.xtable.catalog.CatalogUtils.castToHierarchicalTableIdentifier;

public class HudiGlueCatalogTableBuilder implements CatalogTableBuilder<TableInput, Table> {
  protected static final String GLUE_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
  private final GlueSchemaExtractor schemaExtractor;
  private final HudiTableManager hudiTableManager;
  private final GlueCatalogConfig glueCatalogConfig;
  private HoodieTableMetaClient metaClient;

  public HudiGlueCatalogTableBuilder(
      GlueCatalogConfig glueCatalogConfig, Configuration configuration) {
    this.glueCatalogConfig = glueCatalogConfig;
    this.schemaExtractor = GlueSchemaExtractor.getInstance();
    this.hudiTableManager = HudiTableManager.of(configuration);
  }

  @VisibleForTesting
  HudiGlueCatalogTableBuilder(
      GlueCatalogConfig glueCatalogConfig,
      GlueSchemaExtractor schemaExtractor,
      HudiTableManager hudiTableManager,
      HoodieTableMetaClient metaClient) {
    this.glueCatalogConfig = glueCatalogConfig;
    this.hudiTableManager = hudiTableManager;
    this.schemaExtractor = schemaExtractor;
    this.metaClient = metaClient;
  }

  HoodieTableMetaClient getMetaClient(String basePath) {
    if (metaClient == null) {
      Optional<HoodieTableMetaClient> metaClientOpt =
          hudiTableManager.loadTableMetaClientIfExists(basePath);

      if (!metaClientOpt.isPresent()) {
        throw new CatalogSyncException(
            "failed to get meta client since table is not present in the base path " + basePath);
      }

      metaClient = metaClientOpt.get();
    }
    return metaClient;
  }

  @Override
  public TableInput getCreateTableRequest(InternalTable table, CatalogTableIdentifier catalogTableIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = castToHierarchicalTableIdentifier(catalogTableIdentifier);
    final Instant now = Instant.now();
    List<String> partitionFields =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(getTableParameters(partitionFields, table.getReadSchema()))
        .partitionKeys(getSchemaPartitionKeys(table.getPartitioningFields()))
        .storageDescriptor(getStorageDescriptor(table))
        .lastAccessTime(now)
        .lastAnalyzedTime(now)
        .build();
  }

  @Override
  public TableInput getUpdateTableRequest(
      InternalTable table, Table glueTable, CatalogTableIdentifier catalogTableIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = castToHierarchicalTableIdentifier(catalogTableIdentifier);
    List<String> partitionFields =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    Map<String, String> tableParameters = new HashMap<>(glueTable.parameters());
    tableParameters.putAll(getTableParameters(partitionFields, table.getReadSchema()));
    List<Column> newColumns = getSchemaWithoutPartitionKeys(table);
    StorageDescriptor sd = glueTable.storageDescriptor();
    StorageDescriptor partitionSD = sd.copy(copySd -> copySd.columns(newColumns));

    final Instant now = Instant.now();
    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(glueTable.tableType())
        .parameters(tableParameters)
        .partitionKeys(getSchemaPartitionKeys(table.getPartitioningFields()))
        .storageDescriptor(partitionSD)
        .lastAccessTime(now)
        .lastAnalyzedTime(now)
        .build();
  }

  @VisibleForTesting
  Map<String, String> getTableParameters(List<String> partitionFields, InternalSchema schema) {
    Map<String, String> sparkTableProperties =
        HudiSparkDataSourceTableUtils.getSparkTableProperties(
            partitionFields, "", glueCatalogConfig.getSchemaLengthThreshold(), schema);
    return new HashMap<>(sparkTableProperties);
  }

  @VisibleForTesting
  StorageDescriptor getStorageDescriptor(InternalTable table) {
    HoodieFileFormat baseFileFormat =
        getMetaClient(table.getBasePath()).getTableConfig().getBaseFileFormat();
    SerDeInfo serDeInfo =
        SerDeInfo.builder()
            .serializationLibrary(getSerDeClassName(baseFileFormat))
            .parameters(getSerdeProperties(false, table.getBasePath()))
            .build();
    return StorageDescriptor.builder()
        .serdeInfo(serDeInfo)
        .location(table.getBasePath())
        .inputFormat(getInputFormatClassName(baseFileFormat, false))
        .outputFormat(getOutputFormatClassName(baseFileFormat))
        .columns(getSchemaWithoutPartitionKeys(table))
        .build();
  }

  List<Column> getSchemaWithoutPartitionKeys(InternalTable table) {
    List<String> partitionKeys =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    return schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema()).stream()
        .filter(c -> !partitionKeys.contains(c.name()))
        .collect(Collectors.toList());
  }

  List<Column> getSchemaPartitionKeys(List<InternalPartitionField> partitioningFields) {
    return partitioningFields.stream()
        .map(
            field -> {
              String fieldName = field.getSourceField().getName();
              String fieldType =
                  schemaExtractor.toTypeString(
                      field.getSourceField().getSchema(), TableFormat.HUDI);
              return Column.builder().name(fieldName).type(fieldType).build();
            })
        .collect(Collectors.toList());
  }

  Map<String, String> getSerdeProperties(boolean readAsOptimized, String basePath) {
    Map<String, String> serdeProperties = new HashMap<>();
    serdeProperties.put(ConfigUtils.TABLE_SERDE_PATH, basePath);
    serdeProperties.put(ConfigUtils.IS_QUERY_AS_RO_TABLE, String.valueOf(readAsOptimized));
    return serdeProperties;
  }
}