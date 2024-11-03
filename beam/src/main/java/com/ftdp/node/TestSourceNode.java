package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.engine.FlowEnv;
import org.apache.beam.sdk.io.hcatalog.HCatToRow;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.*;

public class TestSourceNode extends SourceNode {
    PCollection<Row> source;

    public TestSourceNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    public void init() {
        // 从Hive表读取数据
        Schema.Builder valueSchemaBuilder = Schema.builder();
        Schema.Field aid = Schema.Field.of("aid", Schema.FieldType.INT64);
        Schema.Field exp_pv = Schema.Field.of("exp_pv", Schema.FieldType.INT64);

        Schema schema = valueSchemaBuilder.addField(aid).addField(exp_pv).build();

        Row valueRow1 = Row.withSchema(schema)
                .addValue(123L)
                .addValue(1L)
                .build();
        Row valueRow2 = Row.withSchema(schema)
                .addValue(123L)
                .addValue(1L)
                .build();
        List<Row> data = Arrays.asList(valueRow1, valueRow2);
        source =
                env.getPipeline()
                        .apply(
                                Create.of(data)
                        ).setRowSchema(schema);
    }

    @Override
    public PCollection<Row> getOutput() {
        return source;
    }

    @Override
    public PCollection<Row> getInput() {
        return null;
    }
}
