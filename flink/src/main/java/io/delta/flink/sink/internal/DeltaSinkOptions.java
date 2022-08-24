package io.delta.flink.sink.internal;

import java.util.HashMap;
import java.util.Map;

import io.delta.flink.internal.options.DeltaConfigOption;

public class DeltaSinkOptions {
    public static final Map<String, DeltaConfigOption<?>> USER_FACING_SINK_OPTIONS =
        new HashMap<>();
}
