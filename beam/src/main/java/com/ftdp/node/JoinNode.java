package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.beam.util.MergeRowUtil;
import com.ftdp.engine.FlowEnv;
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
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class JoinNode extends BaseFlowNode {
    FlowNode leftNode;
    FlowNode rightNode;
    String joinType;

    // 定义TupleTag用于标记不同的数据集
    TupleTag<Row> leftTag = new TupleTag<>();
    TupleTag<Row> rightTag = new TupleTag<>();

    Set<String> joinkKeys = StreamSupport
            .stream(nodeInfo.get("join_keys").spliterator(), false)
            .map(m -> m.asText()).collect(Collectors.toSet());

    public JoinNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    public void init() {
        parents.forEach(p -> {
            if (p.getNodeId() == nodeInfo.get("left_node_id").asText()) {
                leftNode = p;
            } else if (p.getNodeId() == nodeInfo.get("right_node_id").asText()) {
                rightNode = p;
            }
        });
        joinType = nodeInfo.get("join_type").asText();
    }

    @Override
    public PCollection<Row> getOutput() {

        // 使用CoGroupByKey转换进行join操作
        return KeyedPCollectionTuple
                .of(leftTag, leftNode.getOutput().apply(getVia()))
                .and(rightTag, rightNode.getOutput().apply(getVia()))
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new ProcessJoinResultFn()));

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
    private MapElements<Row, KV<Row, Row>> getVia() {
        return MapElements.via(
                new SimpleFunction<Row, KV<Row, Row>>() {
                    @Override
                    public KV<Row, Row> apply(Row s) {

                        Schema schema = s.getSchema();

                        List<Object> keyList = new ArrayList<>();
                        Schema.Builder keySchemaBuilder = Schema.builder();

                        List<Object> valueList = new ArrayList<>();
                        Schema.Builder valueSchemaBuilder = Schema.builder();

                        for (int i = 0; i < s.getFieldCount(); i++) {
                            Schema.Field schemaField = schema.getField(i);
                            Object value = s.getValue(i);
                            if (joinkKeys.contains(schema.getField(i).getName())) {
                                keySchemaBuilder.addField(schemaField);
                                keyList.add(value);
                            } else {
                                valueSchemaBuilder.addField(schemaField);
                                valueList.add(value);
                            }
                        }

                        Row keyRow = Row.withSchema(keySchemaBuilder.build()).addValues(keyList).build();
                        Row valueRow = Row.withSchema(valueSchemaBuilder.build()).addValues(valueList).build();

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
