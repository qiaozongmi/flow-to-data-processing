package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.engine.FlowEnv;


public class NodeFactory {
    public static FlowNode createNode(JsonNode nodeInfo, FlowEnv env) {
        FlowNode sourceNode = null;
        try{
            sourceNode = Class
                    .forName(nodeInfo.get("class").asText())
                    .asSubclass(FlowNode.class)
                    .getConstructor(FlowNode.class)
                    .newInstance(env, nodeInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceNode;
    }
    public static FlowNode createSourceNode(JsonNode nodeInfo, FlowEnv env) {
        SourceNode sourceNode = null;
        try{
            sourceNode = Class
                    .forName(nodeInfo.get("class").asText())
                    .asSubclass(SourceNode.class)
                    .getConstructor(SourceNode.class)
                    .newInstance(env, nodeInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceNode;
    }
    public static FlowNode createSinkNode(JsonNode nodeInfo, FlowEnv env) {
        FlowNode sourceNode = null;
        try{
            sourceNode = Class
                    .forName(nodeInfo.get("class").asText())
                    .asSubclass(FlowNode.class)
                    .getConstructor(FlowNode.class)
                    .newInstance(env, nodeInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceNode;
    }
}
