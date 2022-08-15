package io.delta.flink.sink.internal.writer.stats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonFileStats {
    /**
     * Factory class that instantiates appropriate {@link JsonStat} based on the delta
     * {@link DataType}.
     */
    static class JsonStatFactory {

        private final ObjectMapper objectMapper;

        JsonStatFactory(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        public JsonStat newJsonStat(Statistics<?> stat) {
            if (stat == null) {
                return new JsonStatNoop(objectMapper, stat);
            }
            switch (stat.type().getPrimitiveTypeName()) {
                case BINARY:
                    return new JsonStatBinary(objectMapper, (BinaryStatistics) stat);
                case INT32:
                    return new JsonStatInteger(objectMapper, (IntStatistics) stat);
                case INT64:
                    return new JsonStatLong(objectMapper, (LongStatistics) stat);
                case DOUBLE:
                    return new JsonStatDouble(objectMapper, (DoubleStatistics) stat);
                case FLOAT:
                    return new JsonStatFloat(objectMapper, (FloatStatistics) stat);
                default:
                    return new JsonStatNoop(objectMapper, stat);
            }
        }
    }

    /**
     * Base class for stats of all data types.
     * <p>
     * To add stat support for more data types, add an implementation that derives from JsonStat and
     * add to {@link JsonStatFactory}.
     */
    abstract static class JsonStat {

        public static final Logger LOG = LoggerFactory.getLogger(JsonStat.class);
        protected ObjectMapper objectMapper;
        private final Statistics<?> stat;

        JsonStat(ObjectMapper objectMapper, Statistics<?> stat) {
            this.objectMapper = objectMapper;
            this.stat = stat;
        }

        public abstract JsonNode getMin();

        public abstract JsonNode getMax();

        public JsonNode getNullCount() {
            return objectMapper.getNodeFactory().numberNode(stat.getNumNulls());
        }
    }

    /**
     * Generates json stats for binary columns
     */
    static class JsonStatBinary extends JsonStat {

        private final BinaryStatistics stat;

        JsonStatBinary(ObjectMapper objectMapper, BinaryStatistics stat) {
            super(objectMapper, stat);
            this.stat = stat;
        }

        @Override
        public JsonNode getMin() {
            return objectMapper.getNodeFactory().binaryNode(stat.genericGetMin().getBytes());
        }

        @Override
        public JsonNode getMax() {
            return objectMapper.getNodeFactory().binaryNode(stat.genericGetMax().getBytes());
        }
    }

    /**
     * Generates json stats for double column
     */
    static class JsonStatDouble extends JsonStat {

        private final DoubleStatistics stat;

        JsonStatDouble(ObjectMapper objectMapper, DoubleStatistics stat) {
            super(objectMapper, stat);
            this.stat = stat;
        }

        @Override
        public JsonNode getMin() {
            return objectMapper.getNodeFactory().numberNode(stat.getMin());
        }

        @Override
        public JsonNode getMax() {
            return objectMapper.getNodeFactory().numberNode(stat.getMax());
        }
    }

    /** Generates json stats for float columns */
    static class JsonStatFloat extends JsonStat {

        private final FloatStatistics stat;

        JsonStatFloat(ObjectMapper objectMapper, FloatStatistics stat) {
            super(objectMapper, stat);
            this.stat = stat;
        }

        @Override
        public JsonNode getMin() {
            return objectMapper.getNodeFactory().numberNode(stat.getMin());
        }

        @Override
        public JsonNode getMax() {
            return objectMapper.getNodeFactory().numberNode(stat.getMax());
        }
    }

    /** Generates json stats for integer columns */
    static class JsonStatInteger extends JsonStat {

        private final IntStatistics stat;

        JsonStatInteger(ObjectMapper objectMapper, IntStatistics stat) {
            super(objectMapper, stat);
            this.stat = stat;
        }

        @Override
        public JsonNode getMin() {
            return objectMapper.getNodeFactory().numberNode(stat.getMin());
        }

        @Override
        public JsonNode getMax() {
            return objectMapper.getNodeFactory().numberNode(stat.getMax());
        }
    }

    /** Generates json stats for long columns */
    static class JsonStatLong extends JsonStat {

        private final LongStatistics stat;

        JsonStatLong(ObjectMapper objectMapper, LongStatistics stat) {
            super(objectMapper, stat);
            this.stat = stat;
        }

        @Override
        public JsonNode getMin() {
            return objectMapper.getNodeFactory().numberNode(stat.getMin());
        }

        @Override
        public JsonNode getMax() {
            return objectMapper.getNodeFactory().numberNode(stat.getMax());
        }
    }

    /**
     * Used for data types for which min/max stats are not supported in delta format (e.g. repeated
     * field)
     */
    static class JsonStatNoop extends JsonStat {

        JsonStatNoop(ObjectMapper objectMapper, Statistics<?> stat) {
            super(objectMapper, stat);
        }

        @Override
        public JsonNode getMin() {
            return objectMapper.getNodeFactory().nullNode();
        }

        @Override
        public JsonNode getMax() {
            return objectMapper.getNodeFactory().nullNode();
        }

        @Override
        public JsonNode getNullCount() {
            return objectMapper.getNodeFactory().nullNode();
        }
    }
}
