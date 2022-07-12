package org.example.source.bounded;

import java.util.Arrays;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.utils.ConsoleSink;
import org.utils.Utils;
import org.utils.job.bounded.DeltaBoundedSourceLocalJobExampleBase;

/**
 * Demonstrates how the Flink Delta source can be used to read data from Delta table.
 * <p>
 * If you run this example then application will spawn example local Flink batch job that will read
 * data from Delta table placed under "src/main/resources/data/source_table_no_partitions".
 * Read records will be printed to log using custom Sink Function.
 * <p>
 * This configuration will read only columns specified by user.
 * If any of the columns was a partition column, connector will automatically detect it.
 * Source will read data from the latest snapshot.
 */
public class DeltaBoundedSourceUserColumnsExample extends DeltaBoundedSourceLocalJobExampleBase {

    private static final String TABLE_PATH =
        Utils.resolveExampleTableAbsolutePath("data/source_table_no_partitions");

    private static final RowType ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("f1", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("f3", new IntType())
    ));

    public static void main(String[] args) throws Exception {
        new DeltaBoundedSourceUserColumnsExample().run(TABLE_PATH);
    }

    /**
     * An example of using Flink Delta Source in streaming pipeline.
     */
    @Override
    public StreamExecutionEnvironment createPipeline(
            String tablePath,
            int sourceParallelism,
            int sinkParallelism) {

        DeltaSource<RowData> deltaSink = getDeltaSource(tablePath);
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        env
            .fromSource(deltaSink, WatermarkStrategy.noWatermarks(), "bounded-delta-source")
            .setParallelism(sourceParallelism)
            .addSink(new ConsoleSink(ROW_TYPE))
            .setParallelism(1);

        return env;
    }

    // TODO PR 18 implement .option("columnNames", ...) was missed.
    /**
     * An example of Flink Delta Source configuration that will read only columns specified by user.
     * via {@code .columnNames(...)} method. Alternatively, the {@code .option("columnNames",
     * List<String> names} method can be used.
     */
    @Override
    public DeltaSource<RowData> getDeltaSource(String tablePath) {
        return DeltaSource.forBoundedRowData(
                new Path(tablePath),
                new Configuration()
            )
            .columnNames("f1", "f3")
            .build();
    }
}
