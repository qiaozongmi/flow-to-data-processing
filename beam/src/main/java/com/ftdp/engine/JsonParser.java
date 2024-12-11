package com.ftdp.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ftdp.node.FlowNode;
import com.ftdp.node.NodeFactory;
import com.ftdp.node.SinkNode;
import com.ftdp.node.SourceNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class JsonParser {
    List<JsonNode> sourceNodeList = new ArrayList<>();
    List<JsonNode> processorNodeList = new ArrayList<>();
    List<JsonNode> sinkNodeList = new ArrayList<>();
    Map<String, JsonNode> allJsonNodes = new HashMap<>();
    Map<String, FlowNode> processorAndSinkNodeMap = new HashMap<>();
    List<FlowNode> sourceFlowNodeList = new ArrayList<>();

    List<FlowNode> processorFlowNodeList = new ArrayList<>();

    List<FlowNode> sinkFlowNodeList = new ArrayList<>();

    JsonNode confNode;

    public JsonParser() {
    }

    public void parse(String jsonFile) {

        try {
            InputStream file = Thread.currentThread().getContextClassLoader().getResourceAsStream(jsonFile);
            // 创建ObjectMapper实例
            ObjectMapper objectMapper = new ObjectMapper();

            // 解析JSON字符串
            JsonNode jsonNode = objectMapper.readTree(file);
            //  获取JsonNode的子节点
            StreamSupport
                    .stream(jsonNode.spliterator(), false)
                    .forEach(node -> {
                        allJsonNodes.put(node.get("id").asText(), node);
                        String type = node.get("type").asText();
                        if (type.equals("conf")) {
                            confNode = node;
                        } else if (type.equals("source")) {
                            sourceNodeList.add(node);
                        } else if (type.equals("processor")) {
                            processorNodeList.add(node);
                        } else if (type.equals("sink")) {
                            sinkNodeList.add(node);
                        }
                    });
            // 创建PipelineOptions
            PipelineOptions options = PipelineOptionsFactory.create();
            options.setJobName(confNode.get("job_name").asText(jsonFile));
            options.setRunner((Class<? extends PipelineRunner<?>>) Class.forName(confNode.get("runner").asText()));

            // 创建Pipeline
            Pipeline pipeline = Pipeline.create(options);
            FlowEnv env = new FlowEnv(pipeline);
            // 创建FlowNode
            sourceFlowNodeList = sourceNodeList
                    .stream()
                    .map(node -> NodeFactory.createSourceNode(node, env))
                    .collect(Collectors.toList());
            processorFlowNodeList = processorNodeList
                    .stream()
                    .map(node ->
                            {
                                FlowNode flowNode = NodeFactory.createNode(node, env);
                                processorAndSinkNodeMap.put(flowNode.getNodeId(), flowNode);
                                return flowNode;
                            }
                    ).collect(Collectors.toList());
            sinkFlowNodeList = sinkNodeList
                    .stream()
                    .map(node ->
                    {
                        FlowNode sinkNode = NodeFactory.createSinkNode(node, env);
                        processorAndSinkNodeMap.put(sinkNode.getNodeId(), (FlowNode) sinkNode);
                        return sinkNode;
                    })
                    .collect(Collectors.toList());
            // 添加子节点
            sourceFlowNodeList.stream().forEach(
                    node -> {
                        node.init();
                        addChild(node);
                    });
            processorFlowNodeList.stream().forEach(
                    node -> {
                        node.init();
                        addChild(node);
                    });
            sinkFlowNodeList.stream().forEach(
                    node -> {
                        node.init();
                        SinkNode sinkNode = (SinkNode) node;
                        sinkNode.sink();
                    });
            // 执行Pipeline
            pipeline.run().waitUntilFinish();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addChild(FlowNode node) {
        StreamSupport.stream(node.getNodeInfo().get("wires").get(0).spliterator(), false)
                .forEach(
                        wire -> {
                            FlowNode subNode = processorAndSinkNodeMap.get(wire.asText());
                            subNode.addParent(node);
                            node.addChild(subNode);
                        }
                );
    }

    public static void main(String[] args) {
        JsonParser parser = new JsonParser();
        parser.parse("dwm/tables/aid_join_table.json");
    }
}