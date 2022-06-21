package io.delta.flink.sink.stats;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.flink.sink.internal.writer.stats.DeltaFileStats;
import io.delta.flink.sink.internal.writer.stats.ParquetFileStats;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public class DeltaFileStatsTest {
    private static final String PATH = "/test-data/test-parquet-stats" +
        "/part-d0e3e782-2a5d-49ca-a4d0-3a9df5a8f37c-0.snappy.parquet";

    @Test
    public void testDeltaStats() throws Exception {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + PATH;
        ParquetFileStats stats = ParquetFileStats.readStats(initialTablePath);
        StructType schema = new StructType()
            .add(new StructField("f1", new BinaryType()))
            .add(new StructField("f2", new BinaryType()))
            .add(new StructField("f3", new IntegerType()));
        DeltaFileStats deltaStats = new DeltaFileStats(schema, stats);
        String json = deltaStats.toJson();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode root = objectMapper.readTree(json);
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
}
