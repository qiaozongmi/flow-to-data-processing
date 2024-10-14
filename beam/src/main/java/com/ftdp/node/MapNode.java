package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.engine.FlowEnv;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.lang.reflect.Method;

public class MapNode extends BaseFlowNode {
    public MapNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    @Override
    public PCollection<Row> getOutput() {
        PCollection<Row> collResult = getInput()
                .apply(MapElements.via(
                        new SimpleFunction<Row, Row>() {
                            @Override
                            public Row apply(Row s) {
                                Row result = null;
                                try {
                                    // 使用反射获取类对象
                                    Class clazz = Class.forName(nodeInfo.get("processor_class").asText());
                                    // 获取静态方法
                                    Method method = clazz.getMethod("processElement", Row.class);
                                    result = (Row) method.invoke(s); // 传入null作为静态方法的对象
                                    // 调用静态方法
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                                return result;
                            }
                        }))
        ;

        return collResult;
    }
}
