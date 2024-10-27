package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.beam.func.RowCombiner;
import com.ftdp.beam.util.MergeRowUtil;
import com.ftdp.engine.FlowEnv;
import org.apache.beam.examples.WordCount;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReduceByKeyNode extends BaseFlowNode {
    Set<String> keys = StreamSupport
            .stream(nodeInfo.get("keys").spliterator(), false)
            .map(m -> m.asText()).collect(Collectors.toSet());
    Set<Triplet<String, String, String>> values = StreamSupport
            .stream(nodeInfo.get("values").spliterator(), false)
            .map(m -> Triplet.with(
                    m.get("value_name").asText(),
                    m.get("reduce_method").asText("sum"),
                    m.get("value_alias").asText(m.get("value_name").asText())))
            .collect(Collectors.toSet());

    public ReduceByKeyNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    public PCollection<Row> process(PCollection<Row> input) {
        return input
                .apply(MapElements.via(
                        new SimpleFunction<Row, KV<Row, Row>>() {
                            @Override
                            public KV<Row, Row> apply(Row s) {

                                Schema schema = s.getSchema();

                                List<Object> keyList = new ArrayList<>();
                                Schema.Builder keySchemaBuilder = Schema.builder();

                                keys.stream().forEach(
                                        v -> {
                                            Schema.Field fieldSchema = schema.getField(v);
                                            Schema.Options options = Schema.Options
                                                    .builder()
                                                    .setOption("reduce_method", Schema.FieldType.STRING, v)
                                                    .build();
                                            fieldSchema = fieldSchema.withOptions(options);
                                            keySchemaBuilder.addField(fieldSchema);
                                            keyList.add(s.getValue(v));

                                            Row.withSchema(s.getSchema())
                                                    .withFieldValue(v, s.getValue(v)).build();
                                        }
                                );
                                Row keyRow = Row.withSchema(keySchemaBuilder.build()).addArray(keyList).build();

                                List<Object> valueList = new ArrayList<>();
                                Schema.Builder valueSchemaBuilder = Schema.builder();

                                values.stream().forEach(
                                        v -> {
                                            Schema.Field fieldSchema = schema.getField(v.getValue0());
                                            Schema.Options options = Schema.Options
                                                    .builder()
                                                    .setOption("reduce_method", Schema.FieldType.STRING, v.getValue1())
                                                    .build();
                                            fieldSchema = fieldSchema.withOptions(options).withName(v.getValue2());
                                            valueSchemaBuilder.addField(fieldSchema);
                                            valueList.add(s.getValue(v.getValue0()));
                                        }
                                );
                                Row valueRow = Row.withSchema(valueSchemaBuilder.build()).addArray(valueList).build();

                                return KV.of(keyRow, valueRow);
                            }
                        }))
                .apply(Combine.perKey(new RowCombiner()))
                .apply(MapElements.via(
                        new SimpleFunction<KV<Row, Row>, Row>() {
                            @Override
                            public Row apply(KV<Row, Row> s) {
                                try {
                                    return MergeRowUtil.mergeByMethod(s.getKey(), s.getValue());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }));
    }

    @Override
    public PCollection<Row> getOutput() {
        return process(getInput());
    }

}

