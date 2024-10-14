package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.engine.FlowEnv;
import com.ftdp.node.BaseFlowNode;
import org.apache.beam.sdk.io.hcatalog.HCatToRow;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.HashMap;
import java.util.Map;

public class HiveSourceNode extends SourceNode {
    PCollection<Row> source;

    public HiveSourceNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    public void init() {
        // 从Hive表读取数据
        Map<String, String> configProperties = new HashMap<String, String>();
        configProperties.put("hive.metastore.uris", nodeInfo.get("hive.metastore.uris").asText());
        source =
                env.getPipeline()
                        .apply(
                                HCatToRow.fromSpec(HCatalogIO.read()
                                        .withConfigProperties(configProperties)
                                        .withDatabase(nodeInfo.get("db_name").asText())
                                        .withTable(nodeInfo.get("table_name").asText())
                                        .withFilter("partition_time=20241001")

                                )
                        );
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
