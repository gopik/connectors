package io.delta.flink.sink.internal;

import java.util.HashMap;
import java.util.Map;

import io.delta.flink.options.BooleanOptionTypeConverter;
import io.delta.flink.options.DeltaConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class DeltaSinkOptions {
    public static final Map<String, DeltaConfigOption<?>> USER_FACING_SINK_OPTIONS =
        new HashMap<>();

    public static final DeltaConfigOption<Boolean> READ_PARQUET_STATS =
        DeltaConfigOption.of(
            ConfigOptions.key("read_parquet_stats").booleanType().defaultValue(false),
            Boolean.class,
            new BooleanOptionTypeConverter()
        );


    static {
        USER_FACING_SINK_OPTIONS.put(READ_PARQUET_STATS.key(), READ_PARQUET_STATS);
    }
}
