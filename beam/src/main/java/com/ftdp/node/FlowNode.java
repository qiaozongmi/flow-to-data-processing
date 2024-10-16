package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;

public interface FlowNode {
    public ArrayList<FlowNode> getParents();

    public ArrayList<FlowNode> getChilds();

    public void addParent(FlowNode parent);

    public void addChild(FlowNode child);

    public PCollection<Row> getInput();

    public PCollection<Row> getOutput();
    public PCollection<Row> process(PCollection<Row> input);

    public String getNodeId();

    public JsonNode getNodeInfo();

    public void init();
}
