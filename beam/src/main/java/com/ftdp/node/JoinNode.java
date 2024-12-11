package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.beam.util.MergeRowUtil;
import com.ftdp.engine.FlowEnv;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class JoinNode extends BaseFlowNode implements Serializable {
    FlowNode leftNode;
    FlowNode rightNode;
    String joinType;

    // 定义TupleTag用于标记不同的数据集
    TupleTag<Row> leftTag;
    TupleTag<Row> rightTag;

    Schema leftSchema;

    Schema rightSchema;
    Schema joinKeysSchema;
    Schema leftValueSchema;
    Schema rightValueSchema;
    Schema resultSchema;
    ArrayList<String> joinkKeys;
    ArrayList<Pair<String, String>> leftValues;
    ArrayList<Pair<String, String>> rightValues;


    public JoinNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    public void init() {
        parents.forEach(p -> {
            if (p.getNodeId().equals(nodeInfo.get("left_node_id").asText())) {
                leftNode = p;
            } else if (p.getNodeId().equals(nodeInfo.get("right_node_id").asText())) {
                rightNode = p;
            }
        });
        joinType = nodeInfo.get("join_type").asText();
        leftSchema = leftNode.getOutput().getSchema();
        rightSchema = rightNode.getOutput().getSchema();

        joinkKeys = StreamSupport
                .stream(nodeInfo.get("join_keys").spliterator(), false)
                .map(m -> m.asText())
                .collect(Collectors.toCollection(ArrayList::new));

        leftValues = StreamSupport
                .stream(nodeInfo.get("left_values").spliterator(), false)
                .map(m ->
                        Pair.with(
                                m.get("value_name").asText(),
                                m.get("value_alias").asText(m.get("value_name").asText())
                        )
                )
                .collect(Collectors.toCollection(ArrayList::new));
        rightValues = StreamSupport
                .stream(nodeInfo.get("right_values").spliterator(), false)
                .map(m ->
                        Pair.with(
                                m.get("value_name").asText(),
                                m.get("value_alias").asText(m.get("value_name").asText())
                        )
                )
                .collect(Collectors.toCollection(ArrayList::new));
        leftTag = new TupleTag<Row>() {
        };
        rightTag = new TupleTag<Row>() {

        };

    }

    @Override
    public PCollection<Row> getOutput() {
        Schema.Builder resultSchemaBuilder = Schema.builder();

        Schema.Builder joinKeysS = Schema.builder();
        joinkKeys.stream().forEach(
                v -> {
                    Schema.Field fieldSchema = leftSchema.getField(v);
                    Schema.Options options = Schema.Options
                            .builder()
                            .setOption("alias", Schema.FieldType.STRING, v)
                            .build();
                    fieldSchema = fieldSchema.withOptions(options);
                    joinKeysS.addField(fieldSchema);
                }
        );
        joinKeysSchema = joinKeysS.build();
        Schema.Builder leftValueSB = Schema.builder();
        leftValues.stream().forEach(
                v -> {
                    Schema.Field fieldSchema = leftSchema.getField(v.getValue0());
                    Schema.Options options = Schema.Options
                            .builder()
                            .setOption("origin_name", Schema.FieldType.STRING, v.getValue0())
                            .build();
                    fieldSchema = fieldSchema.withOptions(options).withName(v.getValue1());
                    leftValueSB.addField(fieldSchema);
                    resultSchemaBuilder.addField(fieldSchema);
                }
        );
        leftValueSchema = leftValueSB.build();

        Schema.Builder rightValueSB = Schema.builder();
        rightValues.stream().forEach(
                v -> {
                    Schema.Field fieldSchema = leftSchema.getField(v.getValue0());
                    Schema.Options options = Schema.Options
                            .builder()
                            .setOption("origin_name", Schema.FieldType.STRING, v.getValue0())
                            .build();
                    fieldSchema = fieldSchema.withOptions(options).withName(v.getValue1());
                    rightValueSB.addField(fieldSchema);
                    resultSchemaBuilder.addField(fieldSchema);
                }
        );
        rightValueSchema = rightValueSB.build();
        resultSchema = resultSchemaBuilder.build();
        // 使用CoGroupByKey转换进行join操作
        return KeyedPCollectionTuple
                .of(leftTag, leftNode.getOutput().apply(getVia(joinKeysSchema,leftValueSchema)).setCoder(KvCoder.of(RowCoder.of(joinKeysSchema), RowCoder.of(leftValueSchema))))
                .and(rightTag, rightNode.getOutput().apply(getVia(joinKeysSchema,rightValueSchema)).setCoder(KvCoder.of(RowCoder.of(joinKeysSchema), RowCoder.of(rightValueSchema))))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ProcessJoinResultFn()))
                .setRowSchema(resultSchema);

    }

    // 自定义DoFn用于处理join后的结果
    class ProcessJoinResultFn extends DoFn<KV<Row, CoGbkResult>, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<Row, CoGbkResult> element = c.element();
            Row key = c.element().getKey();
            Iterable<Row> leftRow = element.getValue().getAll(leftTag);
            Iterable<Row> rightRow = element.getValue().getAll(rightTag);
            switch (joinType) {
                case "inner_join":
                    if (leftRow != null && rightRow != null) {
                        extracted(c, element, key, leftRow, rightRow);
                    }
                case "left_join":
                    if (leftRow != null) {
                        extracted(c, element, key, leftRow, rightRow);
                    }
                case "right_join":
                    if (rightRow != null) {
                        extracted(c, element, key, leftRow, rightRow);
                    }
                case "full_outer_join":
                    extracted(c, element, key, leftRow, rightRow);
                default:
                    throw new Exception("join type not supported");
            }

        }

        private void extracted(DoFn<KV<Row, CoGbkResult>, Row>.ProcessContext c, KV<Row, CoGbkResult> element, Row key, Iterable<Row> leftRow, Iterable<Row> rightRow) {
            for (Row left : leftRow) {
                for (Row right : rightRow) {
                    c.output(MergeRowUtil.mergeOnAlias(key, left, right));
                }
            }
            c.output(element.getKey());
        }
    }

    @NotNull
    private MapElements<Row, KV<Row, Row>> getVia(Schema keySchema, Schema valueSchema) {
        return MapElements.via(
                new SimpleFunction<Row, KV<Row, Row>>() {
                    @Override
                    public KV<Row, Row> apply(Row s) {

                        ArrayList<Object> keyList = new ArrayList<>();
                        for (int i = 0; i < keySchema.getFieldCount(); i++) {
                            Object value = s.getValue(keySchema.getField(i).getOptions().getValue("origin_name",String.class));
                            keyList.add(value);
                        }
                        ArrayList<Object> valueList = new ArrayList<>();
                        for (int i = 0; i < valueSchema.getFieldCount(); i++) {
                            Object value = s.getValue(valueSchema.getField(i).getOptions().getValue("origin_name",String.class));
                            valueList.add(value);
                        }


                        Row keyRow = Row.withSchema(keySchema).addValues(keyList).build();
                        Row valueRow = Row.withSchema(valueSchema).addValues(valueList).build();

                        return KV.of(keyRow, valueRow);
                    }
                });
    }

    public static void main(String[] args) {

    }

    // 自定义DoFn用于解析文本行为Row对象
//    static class ParseRowFn extends DoFn<String, Row> {
//        @ProcessElement
//        public void processElement(ProcessContext c) {
//            String[] fields = c.element().split(",");
//            Row row = Row.withSchema(/*定义Row的Schema*/).addValues(/*根据Schema定义添加字段值*/).build();
//            c.output(row);
//        }
//    }


}
