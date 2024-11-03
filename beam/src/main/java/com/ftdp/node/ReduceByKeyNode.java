package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.beam.func.RowCombiner;
import com.ftdp.engine.FlowEnv;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.javatuples.Triplet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReduceByKeyNode extends BaseFlowNode implements Serializable {
    ArrayList<String> keys;
    ArrayList<Triplet<String, String, String>> values;
    Schema keySchema;
    Schema valueSchema;
    Schema resultSchema;

    @Override
    public void init() {
    }

    public ReduceByKeyNode() {
    }

    public ReduceByKeyNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    public PCollection<Row> process(PCollection<Row> input) {
        JsonNode key = nodeInfo.get("keys");
        keys = new ArrayList(Arrays.asList(nodeInfo.get("keys").asText().split(",")));
        values = StreamSupport
                .stream(nodeInfo.get("values").spliterator(), false)
                .map(m -> Triplet.with(
                        m.get("value_name").asText(),
                        m.get("reduce_method").asText("sum"),
                        m.get("value_alias").asText(m.get("value_name").asText())))
                .collect(Collectors.toCollection(ArrayList::new));

        Schema schema = input.getSchema();
        Schema.Builder keySchemaBuilder = Schema.builder();
        Schema.Builder resultSchemaBuilder = Schema.builder();


        keys.stream().forEach(
                v -> {
                    Schema.Field fieldSchema = schema.getField(v);
                    Schema.Options options = Schema.Options
                            .builder()
                            .setOption("reduce_method", Schema.FieldType.STRING, v)
                            .build();
                    fieldSchema = fieldSchema.withOptions(options);
                    keySchemaBuilder.addField(fieldSchema);
                    resultSchemaBuilder.addField(fieldSchema);
                }
        );
        keySchema = keySchemaBuilder.build();
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
                    resultSchemaBuilder.addField(fieldSchema);
                }
        );
        valueSchema = valueSchemaBuilder.build();
        resultSchema = resultSchemaBuilder.build();
        return input
                .apply(MapElements.via(
                        new SimpleFunction<Row, KV<Row, Row>>() {
                            @Override
                            public KV<Row, Row> apply(Row s) {

                                Row.Builder keyRowBuilder = Row.withSchema(keySchema);
                                Row.Builder valueRowBuilder = Row.withSchema(valueSchema);

                                keys.stream().forEach(
                                        v -> {
                                            keyRowBuilder.addValue(s.getValue(v));
                                        }
                                );

                                values.stream().forEach(
                                        v -> {
                                            valueRowBuilder.addValue(s.getValue(v.getValue0()));
                                        }
                                );

                                return KV.of(keyRowBuilder.build(), valueRowBuilder.build());
                            }
                        }))
                .setCoder(KvCoder.of(RowCoder.of(keySchema), RowCoder.of(valueSchema)))
                .apply("", Combine.perKey(new RowCombiner(valueSchema)))
                .apply(MapElements.via(
                        new SimpleFunction<KV<Row, Row>, Row>() {
                            @Override
                            public Row apply(KV<Row, Row> s) {
                                try {
                                    //合并方法
                                    Row.Builder resultBuilder = Row.withSchema(resultSchema);
                                    keys.stream().forEach(
                                            v -> {
                                                resultBuilder.addValue(s.getKey().getValue(v));
                                            }
                                    );
                                    values.stream().forEach(
                                            v -> {
                                                resultBuilder.addValue(s.getValue().getValue(v.getValue2()));
                                            }
                                    );
                                    return resultBuilder.build();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }))
                .setRowSchema(resultSchema);
    }


    @Override
    public PCollection<Row> getOutput() {
        return process(getInput());
    }

}

