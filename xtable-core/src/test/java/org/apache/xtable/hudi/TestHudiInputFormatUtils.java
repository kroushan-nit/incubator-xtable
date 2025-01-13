package org.apache.xtable.hudi;

import org.apache.hudi.common.model.HoodieFileFormat;

import org.apache.xtable.exception.NotSupportedException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHudiInputFormatUtils {

  @Test
  void testGetInputFormatClassName_Parquet() {
    String inputFormat = HudiInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, false);
    assertEquals("org.apache.hudi.hadoop.HoodieParquetInputFormat", inputFormat);

    String realtimeInputFormat = HudiInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, true);
    assertEquals("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat", realtimeInputFormat);
  }

  @Test
  void testGetInputFormatClassName_HFile() {
    String inputFormat = HudiInputFormatUtils.getInputFormatClassName(HoodieFileFormat.HFILE, false);
    assertEquals("org.apache.hudi.hadoop.HoodieHFileInputFormat", inputFormat);

    String realtimeInputFormat = HudiInputFormatUtils.getInputFormatClassName(HoodieFileFormat.HFILE, true);
    assertEquals("org.apache.hudi.hadoop.realtime.HoodieHFileRealtimeInputFormat", realtimeInputFormat);
  }

  @Test
  void testGetInputFormatClassName_Orc() {
    String inputFormat = HudiInputFormatUtils.getInputFormatClassName(HoodieFileFormat.ORC, false);
    assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", inputFormat);
  }

  @Test
  void testGetInputFormatClassName_UnsupportedFormat() {
    Exception exception = assertThrows(NotSupportedException.class, () ->
        HudiInputFormatUtils.getInputFormatClassName(HoodieFileFormat.HOODIE_LOG, false));
    assertTrue(exception.getMessage().contains("Hudi InputFormat not implemented for base file format"));
  }

  @Test
  void testGetOutputFormatClassName() {
    String parquetOutputFormat = HudiInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.PARQUET);
    assertEquals("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", parquetOutputFormat);

    String hfileOutputFormat = HudiInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.HFILE);
    assertEquals("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", hfileOutputFormat);

    String orcOutputFormat = HudiInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.ORC);
    assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat", orcOutputFormat);
  }

  @Test
  void testGetOutputFormatClassName_UnsupportedFormat() {
    Exception exception = assertThrows(NotSupportedException.class, () ->
        HudiInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.HOODIE_LOG));
    assertTrue(exception.getMessage().contains("No OutputFormat for base file format"));
  }

  @Test
  void testGetSerDeClassName() {
    String parquetSerDe = HudiInputFormatUtils.getSerDeClassName(HoodieFileFormat.PARQUET);
    assertEquals("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", parquetSerDe);

    String hfileSerDe = HudiInputFormatUtils.getSerDeClassName(HoodieFileFormat.HFILE);
    assertEquals("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", hfileSerDe);

    String orcSerDe = HudiInputFormatUtils.getSerDeClassName(HoodieFileFormat.ORC);
    assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcSerde", orcSerDe);
  }

  @Test
  void testGetSerDeClassName_UnsupportedFormat() {
    Exception exception = assertThrows(NotSupportedException.class, () ->
        HudiInputFormatUtils.getSerDeClassName(HoodieFileFormat.HOODIE_LOG));
    assertTrue(exception.getMessage().contains("No SerDe for base file format"));
  }
}
