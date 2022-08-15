package io.delta.flink.sink.internal.writer.stats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads column stats from the parquet metadata.
 */
public class ParquetFileStats {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetFileStats.class);


    private final Map<ColumnPath, Statistics<?>> columnStats;
    private long rowCount;

    private ParquetFileStats(ParquetMetadata parquetMetadata) {
        this.columnStats = load(parquetMetadata);
    }

    /**
     * Factory method to read stats given path to parquet file.
     *
     * Parquet metadata is stored in the file footer which contains row group metadata for each row
     * group. Each row group metadata contains column stats for that row group. We merge the stats
     * across the row groups to compute file level stats.
     *
     * @param path Path to a parquet file.
     * @return {@link ParquetFileStats}
     */
    public static ParquetFileStats readStats(String path) throws IOException {
        LOG.info("Reading stats from " + path);
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path);
        InputFile inputFile = HadoopInputFile.fromPath(hadoopPath, new Configuration());
        try (ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata parquetMetadata = parquetFileReader.getFooter();
            return new ParquetFileStats(parquetMetadata);
        }
    }

    public Map<ColumnPath, Statistics<?>> getColumnStats() {
        return columnStats;
    }

    public long getRowCount() {
        return rowCount;
    }

    /** Iterates over row group metadata and merge column stats from different row groups. */
    private Map<ColumnPath, Statistics<?>> load(ParquetMetadata parquetMetadata) {
        Map<ColumnPath, Statistics<?>> columnStats = new HashMap<>();
        MessageType parquetSchema = parquetMetadata.getFileMetaData().getSchema();
        for (BlockMetaData rowGroup : parquetMetadata.getBlocks()) {
            this.rowCount += rowGroup.getRowCount();
            for (int colIdx = 0; colIdx < parquetSchema.getColumns().size(); colIdx++) {
                ColumnChunkMetaData colMetadata = rowGroup.getColumns().get(colIdx);
                if (!columnStats.containsKey(colMetadata.getPath())) {
                    columnStats.put(colMetadata.getPath(), colMetadata.getStatistics());
                } else {
                    columnStats.get(colMetadata.getPath()).mergeStatistics(
                            colMetadata.getStatistics());
                }
            }
        }
        return columnStats;
    }
}
