package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.engine.FlowEnv;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
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

public class HiveSinkNode extends BaseFlowNode implements SinkNode {
    HCatalogIO.Write write;
    List<String> writeFields;

    public HiveSinkNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    public void init() {
        // 从Hive表读取数据
        Map<String, String> configProperties = new HashMap<String, String>();
        configProperties.put("hive.metastore.uris", nodeInfo.get("hive.metastore.uris").asText());
        write = HCatalogIO.write()
                .withConfigProperties(configProperties)
                .withDatabase(nodeInfo.get("db_name").asText())
                .withTable(nodeInfo.get("table_name").asText())
                .withBatchSize(10000);
        writeFields = StreamSupport
                .stream(nodeInfo.get("write_fields").spliterator(), false)
                .map(JsonNode::asText)
                .collect(Collectors.toList());
    }

    @Override
    public PCollection<Row> getOutput() {
        return super.getInput();
    }

    @Override
    public void sink() {
        getInput()
                .apply(MapElements.via(
                        new SimpleFunction<Row, HCatRecord>() {
                            @Override
                            public HCatRecord apply(Row s) {
                                Schema schema = s.getSchema();
                                HCatRecord record = new DefaultHCatRecord(writeFields.size());
                                HCatSchemaUtils.CollectionBuilder schemaBuilder = HCatSchemaUtils.getListSchemaBuilder();
                                for (int i = 0; i < writeFields.size(); i++) {
                                    try {
                                        schemaBuilder.addField(new HCatFieldSchema(writeFields.get(i), new PrimitiveTypeInfo(), ""));
                                    } catch (HCatException e) {
                                        throw new RuntimeException(e);
                                    }
                                    record.set(0, s.getValue(i));
                                }

                                return record;
                            }
                        }))
                .apply(write);

    }
}
