package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.engine.FlowEnv;

public class SourceNode extends BaseFlowNode{
    public SourceNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }
    public void init(){};
}
