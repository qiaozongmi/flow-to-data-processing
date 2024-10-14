package com.ftdp.engine;

import org.apache.beam.sdk.Pipeline;

public class FlowEnv {
    Pipeline pipeline;
    public FlowEnv(Pipeline pip) {
        pipeline = pip;
    }
    public Pipeline getPipeline() {
        return pipeline;
    }
}
