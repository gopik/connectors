package io.delta.flink.sink.internal.writer.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.flink.util.Preconditions.checkNotNull;


import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;


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
    private final Map<Class<? extends DataType>, PrimitiveTypeName> typeMap;
    private final ObjectMapper objectMapper;
    JsonStatFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.typeMap = initializeMapping();
    }
    private Map<Class<? extends DataType>, PrimitiveTypeName> initializeMapping() {
        Map<Class<? extends DataType>, PrimitiveTypeName> typeMap = new HashMap<>();
        for (PrimitiveTypeName primitiveTypeName : PrimitiveTypeName.values()) {
            switch (primitiveTypeName) {
                case DOUBLE:
                    typeMap.put(DoubleType.class, primitiveTypeName);
                    break;
                case FLOAT:
                    typeMap.put(FloatType.class, primitiveTypeName);
                    break;
                case INT32:
                    typeMap.put(IntegerType.class, primitiveTypeName);
                    break;
                case INT64:
                    typeMap.put(LongType.class, primitiveTypeName);
                    break;
                case BINARY:
                    typeMap.put(BinaryType.class, primitiveTypeName);
                    typeMap.put(StringType.class, primitiveTypeName);
                    break;
                case BOOLEAN:
                    typeMap.put(BooleanType.class, primitiveTypeName);
                    break;
            }
        }
        return typeMap;
    }
    @Nullable
    Statistics<?> toParquetStats(
        DataType deltaDataType,
        JsonNode max,
        JsonNode min,
        long nullCount) throws IOException {
        if (!typeMap.containsKey(deltaDataType.getClass())) {
            return null;
        }
        switch (typeMap.get(deltaDataType.getClass())) {
            case BINARY:
                Statistics<?> bs = Statistics.createStats(
                    new PrimitiveType(
                        Repetition.OPTIONAL,
                        PrimitiveTypeName.BINARY, PrimitiveTypeName.BINARY.name()));
                bs.incrementNumNulls(nullCount);
                bs.updateStats(Binary.fromConstantByteArray(min.binaryValue()));
                bs.updateStats(Binary.fromConstantByteArray(max.binaryValue()));
                return bs;
            case INT32:
                Statistics<?> is = Statistics.createStats(new PrimitiveType(
                    Repetition.OPTIONAL,
                    PrimitiveTypeName.INT32,
                    PrimitiveTypeName.INT32.name()
                ));
                is.incrementNumNulls(nullCount);
                is.updateStats(min.intValue());
                is.updateStats(max.intValue());
                return is;
            case INT64:
                Statistics<?> ls = Statistics.createStats(new PrimitiveType(
                    Repetition.OPTIONAL,
                    PrimitiveTypeName.INT64,
                    PrimitiveTypeName.INT64.name()
                ));
                ls.incrementNumNulls(nullCount);
                ls.updateStats(min.longValue());
                ls.updateStats(max.longValue());
                return ls;

            case DOUBLE:
                Statistics<?> ds = Statistics.createStats(new PrimitiveType(
                    Repetition.OPTIONAL,
                    PrimitiveTypeName.DOUBLE,
                    PrimitiveTypeName.DOUBLE.name()
                ));
                ds.incrementNumNulls(nullCount);
                ds.updateStats(min.doubleValue());
                ds.updateStats(max.doubleValue());
                return ds;
            case FLOAT:
                Statistics<?> fs = Statistics.createStats(new PrimitiveType(
                    Repetition.OPTIONAL,
                    PrimitiveTypeName.FLOAT,
                    PrimitiveTypeName.FLOAT.name()
                ));
                fs.incrementNumNulls(nullCount);
                fs.updateStats(min.floatValue());
                fs.updateStats(max.floatValue());
        }
        return null;
    }
    public JsonStat newJsonStat(DataType deltaDataType, Statistics<?> stat) {
        JsonStat.LOG.info("newJsonStat: " + deltaDataType);
        if (!typeMap.containsKey(deltaDataType.getClass())) {
            return new JsonStatNoop(objectMapper, stat);
        }
        switch (typeMap.get(deltaDataType.getClass())) {
            case BINARY:
                return new JsonStatBinary(objectMapper, (BinaryStatistics) stat);
            case INT32:
            case INT64:
                return new JsonStatInteger(objectMapper, (IntStatistics) stat);
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
    private final StructType schema;
    private final ObjectMapper objectMapper;
    private final ParquetFileStats stats;
    private final JsonStatFactory statFactory;

    public DeltaFileStats(StructType schema, ParquetFileStats stats) {
        this.schema = checkNotNull(schema);
        this.stats = stats;
        this.objectMapper = new ObjectMapper();
        this.statFactory = new JsonStatFactory(objectMapper);
    }

    public String toJson() {
        ObjectNode root = objectMapper.createObjectNode();
        root.set("numRecords", objectMapper.getNodeFactory().numberNode(stats.getRowCount()));
        root.set("minValues",
            getStat(schema, new ArrayList<>(),
                (dt, stat) -> statFactory.newJsonStat(dt, stat).getMin()));
        root.set("maxValues",
            getStat(schema, new ArrayList<>(),
                (dt, stat) -> statFactory.newJsonStat(dt, stat).getMax()));
        root.set("nullCounts",
            getStat(schema, new ArrayList<>(),
                (dt, stat) -> statFactory.newJsonStat(dt, stat).getNullCount()));
        try {
            return objectMapper.writeValueAsString(root.toString());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode getStat(DataType dataType, ArrayList<String> path,
        BiFunction<DataType, Statistics<?>, JsonNode> toJsonNode) {
        if (dataType instanceof StructType) {
            StructType struct = (StructType) dataType;
            ObjectNode root = objectMapper.createObjectNode();
            for (StructField field : struct.getFields()) {
                path.add(field.getName());
                JsonNode result = getStat(field.getDataType(), path, toJsonNode);
                // If a stat result is null, say if it's not applicable to certain data types
                // or the data type is not supported, skip adding to the json.
                if (!result.isNull()) {
                    root.set(field.getName(), result);
                }
                path.remove(path.size() -1);
            }
            return root;
        } else {
            return toJsonNode.apply(dataType,
                stats.getColumnStats().get(ColumnPath.get(path.toArray(new String[0]))));
        }
    }
    private void parseStats(
        DataType dataType,
        JsonNode deltaStats,
        ArrayList<String> path,
        Map<ColumnPath, Statistics<?>> parquetStatsMap) throws IOException {
        if (dataType instanceof StructType) {
            for (StructField field : ((StructType) dataType).getFields()) {
                path.add(field.getName());
                parseStats(field.getDataType(), deltaStats, path, parquetStatsMap);
                path.remove(path.size() -1);
            }
        } else {
            String fieldPath = String.join("/", path);
            Statistics<?> parquetStats = statFactory.toParquetStats(
                dataType,
                deltaStats.at("/maxValues/" + fieldPath),
                deltaStats.at("/minValues/" + fieldPath),
                deltaStats.at("/nullCounts/" + fieldPath).asLong()
                );
            if (parquetStats != null) {
                ColumnPath columnPath = ColumnPath.get(path.toArray(new String[0]));
                parquetStatsMap.put(columnPath, parquetStats);
            }
        }
    }

    public ParquetFileStats fromJson(String stats) {
        try {
            JsonNode root = objectMapper.readTree(stats);
            long numRecords = root.at("/numRecords").asLong();
            Map<ColumnPath, Statistics<?>> parquetStats = new HashMap<>();
            parseStats(schema, root, new ArrayList<>(), parquetStats);
            return new ParquetFileStats(numRecords, parquetStats);
        } catch (IOException e) {
            JsonStat.LOG.error(stats, e);
            throw new RuntimeException(e);
        }
    }
}
