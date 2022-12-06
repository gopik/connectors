package io.delta.flink.sink.internal.writer.stats;

import java.util.ArrayList;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Converts Parquet stats to delta json format.
 * <p>
 * The referenced columns are not delta logical columns but the physical columns within the parquet
 * file. For a table with following schema:
 * <pre>
 * |-- a: struct
 * |    |-- b: struct
 * |    |    |-- c: long
 * |-- d: int
 * </pre>
 * <p>
 * parquet stats would be captured as:
 *
 * <pre>
 * recordCount: 1000
 * a.b.c, LongStatistics{min=10, max=2000, nullCount=0}
 * d,     IntStatistics{min=5, max=5, nullCount=20}
 *
 * This implementation recreates the nested structure required for delta stats which then become:
 * {
 *   "numRecords": 1000,
 *   "minValues": {
 *     "a": {
 *       "b": {
 *         "c": 10
 *       }
 *     },
 *     "d": 5
 *   },
 *   "maxValues": {
 *     "a": {
 *       "b": {
 *         "c": 2000
 *       }
 *     },
 *     "d": 5
 *   },
 *   "nullCounts": {
 *     "a": {
 *       "b": {
 *         "c": 0
 *       }
 *     },
 *     "d": 20
 *   }
 * }
 * </pre>
 * This generates a Null json node which is then ignored and not added to the final json. NullCounts
 * are still supported.
 */

public class DeltaFileStats {

    private final ObjectMapper objectMapper;
    private final ParquetFileStats stats;
    private final JsonFileStats.JsonStatFactory statFactory;
    private final MessageType messageType;

    public DeltaFileStats(ParquetFileStats stats) {
        this.messageType = stats.getSchema();
        this.stats = stats;
        this.objectMapper = new ObjectMapper();
        this.statFactory = new JsonFileStats.JsonStatFactory(objectMapper);
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
            // Delta log requires stat json to be escaped as a string, hence we need 2
            // level serialization.
            return objectMapper.writeValueAsString(root.toString());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode getStat(
        Type parquetType,
        ArrayList<String> path,
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
                path.remove(path.size() - 1);
            }
            return root;
        } else {
            return toJsonNode.apply(stats.getColumnStats().get(
                ColumnPath.get(path.toArray(new String[0]))));
        }
    }
}
