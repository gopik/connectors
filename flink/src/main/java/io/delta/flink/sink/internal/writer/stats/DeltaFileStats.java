package io.delta.flink.sink.internal.writer.stats;

import java.util.ArrayList;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.util.ParquetSchemaConverter;


/**
 * Base class for stats of all data types.
 *
 * To add stat support for more data types, add an implementation that derives from JsonStat and
 * add to {@link JsonStatFactory}.
 */
abstract class JsonStat {
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
 * Used for data types for which min/max stats are not supported in delta format
 * (eg. repeated field).
 *
 * This generates a Null json node which is then ignored and not added to the final json.
 * NullCounts are still supported.
 */
class JsonStatNoop extends JsonStat {
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
}

/**
 * Factory class that instantiates appropriate {@link JsonStat} based on the delta {@link DataType}.
 */
class JsonStatFactory {
    private final ObjectMapper objectMapper;
    JsonStatFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    public JsonStat newJsonStat(Statistics<?> stat) {
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

class JsonStatFloat extends JsonStat {
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
class JsonStatDouble extends JsonStat {
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

class JsonStatInteger extends JsonStat {
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

class JsonStatLong extends JsonStat {
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

class JsonStatBinary extends JsonStat {
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
 * Converts Parquet stats to delta json format (and vice versa).
 *
 * The referenced columns are not delta logical columns but the physical columns within the parquet
 * file.
 */
public class DeltaFileStats {

    private final ObjectMapper objectMapper;
    private final ParquetFileStats stats;
    private final JsonStatFactory statFactory;
    private final MessageType messageType;

    public DeltaFileStats(StructType schema, ParquetFileStats stats) {
        this.messageType = ParquetSchemaConverter.deltaToParquet(schema);
        this.stats = stats;
        this.objectMapper = new ObjectMapper();
        this.statFactory = new JsonStatFactory(objectMapper);
    }

    public String toJson() {
        ObjectNode root = objectMapper.createObjectNode();
        root.set("numRecords", objectMapper.getNodeFactory().numberNode(stats.getRowCount()));
        root.set("minValues",
            getStat(messageType, new ArrayList<>(),
                stat -> statFactory.newJsonStat(stat).getMin()));
        root.set("maxValues",
            getStat(messageType, new ArrayList<>(),
                stat -> statFactory.newJsonStat(stat).getMax()));
        root.set("nullCounts",
            getStat(messageType, new ArrayList<>(),
                stat -> statFactory.newJsonStat(stat).getNullCount()));
        try {
            return objectMapper.writeValueAsString(root.toString());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    private JsonNode getStat(Type parquetType, ArrayList<String> path,
        Function<Statistics<?>, JsonNode> toJsonNode) {
        if (!parquetType.isPrimitive()) {
            ObjectNode root = objectMapper.createObjectNode();
            for (Type field : parquetType.asGroupType().getFields()) {
                path.add(field.getName());
                JsonNode result = getStat(field, path, toJsonNode);
                // If a stat result is null, say if it's not applicable to certain data types
                // or the data type is not supported, skip adding to the json.
                if (!result.isNull()) {
                    root.set(field.getName(), result);
                }
                path.remove(path.size() -1);
            }
            return root;
        } else {
            return toJsonNode.apply(stats.getColumnStats().get(
                ColumnPath.get(path.toArray(new String[0]))));
        }
    }

    public ParquetFileStats fromJson(String stats) {
        return null;
    }
}
