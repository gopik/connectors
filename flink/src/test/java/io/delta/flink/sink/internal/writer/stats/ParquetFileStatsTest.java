package io.delta.flink.sink.internal.writer.stats;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

import com.google.common.collect.ImmutableList;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * Tests for {@link ParquetFileStats}
 */
public class ParquetFileStatsTest {

    private static final String PATH = "/test-data/test-parquet-stats" +
        "/part-d0e3e782-2a5d-49ca-a4d0-3a9df5a8f37c-0.snappy.parquet";

    @Test
    public void testParquetStatsFromFile_fieldTypes() throws Exception {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + PATH;
        ParquetFileStats stats = ParquetFileStats.readStats(initialTablePath);
        assertEquals(3, stats.getColumnStats().size());
        assertIterableEquals(
            ImmutableList.of(ColumnPath.get("f1"), ColumnPath.get("f2"), ColumnPath.get("f3")),
            stats.getColumnStats().keySet());
        assertEquals(PrimitiveTypeName.BINARY,
            stats.getColumnStats().get(ColumnPath.get("f1")).type().getPrimitiveTypeName());
        assertEquals(PrimitiveTypeName.BINARY,
            stats.getColumnStats().get(ColumnPath.get("f2")).type().getPrimitiveTypeName());
        assertEquals(PrimitiveTypeName.INT32,
            stats.getColumnStats().get(ColumnPath.get("f3")).type().getPrimitiveTypeName());
    }

    @Test
    public void testParquetStatsFromFile_nullCount() throws Exception {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + PATH;
        ParquetFileStats stats = ParquetFileStats.readStats(initialTablePath);
        assertEquals(0, stats.getColumnStats().get(ColumnPath.get("f1")).getNumNulls());
        assertEquals(0, stats.getColumnStats().get(ColumnPath.get("f2")).getNumNulls());
        assertEquals(0, stats.getColumnStats().get(ColumnPath.get("f3")).getNumNulls());
    }

    @Test
    public void testParquetStatsFromFile_minMax() throws Exception {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + PATH;
        ParquetFileStats stats = ParquetFileStats.readStats(initialTablePath);
        // min
        assertEquals("MA==", Base64.getEncoder()
            .encodeToString(stats.getColumnStats().get(ColumnPath.get("f1")).getMinBytes()));
        assertEquals("MTc=", Base64.getEncoder()
            .encodeToString(stats.getColumnStats().get(ColumnPath.get("f2")).getMinBytes()));
        assertEquals(7,
            ByteBuffer.wrap(stats.getColumnStats().get(ColumnPath.get("f3")).getMinBytes()).order(
                    ByteOrder.LITTLE_ENDIAN)
                .getInt());
        // max
        assertEquals("OQ==", Base64.getEncoder()
            .encodeToString(stats.getColumnStats().get(ColumnPath.get("f1")).getMaxBytes()));
        assertEquals("ODc=", Base64.getEncoder()
            .encodeToString(stats.getColumnStats().get(ColumnPath.get("f2")).getMaxBytes()));
        assertEquals(23,
            ByteBuffer.wrap(stats.getColumnStats().get(ColumnPath.get("f3")).getMaxBytes())
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt());
    }
}
