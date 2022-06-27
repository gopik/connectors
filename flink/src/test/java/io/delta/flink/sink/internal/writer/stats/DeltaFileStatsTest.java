package io.delta.flink.sink.internal.writer.stats;

import java.io.File;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public class DeltaFileStatsTest {
    private static final String PATH = "/test-data/test-parquet-stats" +
        "/part-d0e3e782-2a5d-49ca-a4d0-3a9df5a8f37c-0.snappy.parquet";

    @Test
    public void testDeltaStats_path1() throws Exception {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + PATH;
        ParquetFileStats stats = ParquetFileStats.readStats(initialTablePath);
        StructType schema = new StructType()
            .add(new StructField("f1", new BinaryType()))
            .add(new StructField("f2", new StringType()))
            .add(new StructField("f3", new IntegerType()));
        DeltaFileStats deltaStats = new DeltaFileStats(schema, stats);
        String json = deltaStats.toJson();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode root = objectMapper.readTree(objectMapper.readTree(json).asText());
        assertEquals(6, root.at("/numRecords").asLong());

        // nullCounts
        assertEquals(0, root.at("/nullCounts/f1").asLong());
        assertEquals(0, root.at("/nullCounts/f2").asLong());
        assertEquals(0, root.at("/nullCounts/f3").asLong());

        // minValues
        assertEquals("MA==", root.at("/minValues/f1").asText());
        assertEquals("MTc=", root.at("/minValues/f2").asText());
        assertEquals(7, root.at("/minValues/f3").asLong());

        // maxValues
        assertEquals("OQ==", root.at("/maxValues/f1").asText());
        assertEquals("ODc=", root.at("/maxValues/f2").asText());
        assertEquals(23, root.at("/maxValues/f3").asLong());
    }

    @Test
    public void testToParquetStats() throws Exception {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + PATH;
        ParquetFileStats stats = ParquetFileStats.readStats(initialTablePath);
        StructType schema = new StructType()
            .add(new StructField("f1", new BinaryType()))
            .add(new StructField("f2", new StringType()))
            .add(new StructField("f3", new IntegerType()));
        DeltaFileStats deltaStats = new DeltaFileStats(schema, stats);
        String json = deltaStats.toJson();
        DeltaFileStats deltaFileStats = new DeltaFileStats(schema, null);
        stats = deltaFileStats.fromJson(new ObjectMapper().readTree(json).asText());

        Map<ColumnPath, Statistics<?>> columnStats = stats.getColumnStats();

        Encoder encoder = Base64.getEncoder();
        ColumnPath f1 = ColumnPath.get("f1");
        ColumnPath f2 = ColumnPath.get("f2");
        ColumnPath f3 = ColumnPath.get("f3");


        assertEquals("MA==",encoder.encodeToString(columnStats.get(f1).getMinBytes()));
        assertEquals("OQ==", encoder.encodeToString(columnStats.get(f1).getMaxBytes()));
        assertEquals(0, columnStats.get(f1).getNumNulls());
        assertEquals("MTc=", encoder.encodeToString(columnStats.get(f2).getMinBytes()));
        assertEquals("ODc=", encoder.encodeToString(columnStats.get(f2).getMaxBytes()));
        assertEquals(0, columnStats.get(f2).getNumNulls());
        assertEquals(7, columnStats.get(f3).genericGetMin());
        assertEquals(23, columnStats.get(f3).genericGetMax());
        assertEquals(0, columnStats.get(f3).getNumNulls());
    }
}
