package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.engine.FlowEnv;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;

import static java.util.stream.Collectors.toList;

public abstract class BaseFlowNode implements FlowNode {
    ArrayList<FlowNode> parents = new ArrayList<>();
    ArrayList<FlowNode> childs = new ArrayList<>();

    ArrayList<String> parentIdList = new ArrayList<>();
    ArrayList<String> childIdList = new ArrayList<>();
    String nodeId;

    NodeType nodeType;

    FlowEnv env;

    JsonNode nodeInfo;

    public BaseFlowNode(FlowEnv env, JsonNode nodeInfo) {
        this.env = env;
        this.nodeInfo = nodeInfo;
        this.nodeId = nodeInfo.get("id").asText();
        this.nodeType = NodeType.valueOf(nodeInfo.get("type").asText());
    }

    @Override
    public ArrayList<FlowNode> getParents() {
        return parents;
    }

    @Override
    public ArrayList<FlowNode> getChilds() {
        return childs;
    }
    @Override
    public String getNodeId(){
        return nodeId;
    }

    @Override
    public void addParent(FlowNode parent) {
        this.parents.add(parent);
        this.parentIdList.add(parent.getNodeId());
    }

    @Override
    public void addChild(FlowNode child) {
        this.childs.add(child);
        this.parentIdList.add(child.getNodeId());
    }

    @Override
    public PCollection<Row> getInput() {
        Iterable<PCollection<Row>> inputList = getParents().stream()
                .map(node -> node.getOutput())
                .collect(toList());
        PCollection<Row> result = PCollectionList
                .of(inputList)
                .apply("Merge", Flatten.pCollections());

        return result;
    }
    @Override
    public PCollection<Row> getOutput() {
        return null;
    }
    public JsonNode getNodeInfo(){
        return nodeInfo;
    }
    public void init(){
    }

    @Override
    public PCollection<Row> process(PCollection<Row> input) {
        return null;
    }
}
