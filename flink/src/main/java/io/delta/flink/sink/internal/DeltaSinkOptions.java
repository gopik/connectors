package io.delta.flink.sink.internal;

import java.util.HashMap;
import java.util.Map;

import io.delta.flink.internal.options.BooleanOptionTypeConverter;
import io.delta.flink.internal.options.DeltaConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * This class contains all available options for {@link io.delta.flink.sink.DeltaSink} with
 * their type and default values.
 */
public class DeltaSinkOptions {
    /**
     * A map of all valid {@code DeltaSinkOptions}. This map can be used for example by {@code
     * RowDataDeltaSinkBuilder} to do configuration sanity check.
     */
    public static final Map<String, DeltaConfigOption<?>> USER_FACING_SINK_OPTIONS =
        new HashMap<>();

    /**
     * A map of all {@code DeltaSinkOptions} that are internal only, meaning that they must not be
     * used by end user through public API. This map can be used for example by {@code
     * RowDataDeltaSinkBuilder} to do configuration sanity check.
     */
    public static final Map<String, DeltaConfigOption<?>> INNER_SINK_OPTIONS = new HashMap<>();

    /**
     * An option used to enable (or disable) computing of file level stats.
     * <p>
     * If this option is set to true, delta table writes will compute file level stats and add it to
     * the transaction log.
     * <p>
     * <p>
     * The string representation for this option ios <b>computeDeltaStats</b> and its default value
     * is false.
     */
    public static final DeltaConfigOption<Boolean> COMPUTE_DELTA_STATS =
            DeltaConfigOption.of(
                    ConfigOptions.key("computeDeltaStats").booleanType().defaultValue(false),
                    Boolean.class,
                    new BooleanOptionTypeConverter());

    static {
        USER_FACING_SINK_OPTIONS.put(COMPUTE_DELTA_STATS.key(), COMPUTE_DELTA_STATS);
    }
}
