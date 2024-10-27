package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.engine.FlowEnv;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PrintSinkNode extends BaseFlowNode implements SinkNode {

    public PrintSinkNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    @Override
    public PCollection<Row> getOutput() {
        return super.getInput();
    }

    @Override
    public void sink() {
        getInput()
                .apply(ParDo.of(new DoFn<Row, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println(c.element().toString());
                    }
                }));

    }
}
