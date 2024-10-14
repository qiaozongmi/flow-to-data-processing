package com.ftdp.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.ftdp.engine.FlowEnv;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WindowNode extends ReduceByKeyNode {
    long windowSize;

    public WindowNode(FlowEnv env, JsonNode nodeInfo) {
        super(env, nodeInfo);
    }

    public void init() {
        windowSize = nodeInfo.get("window_size").asLong();
    }

    public PCollection<Row> process(PCollection<Row> input) {
        return input
                .apply(ParDo.of(new AddTimestampFn()))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(windowSize))));
    }
    @Override
    public PCollection<Row> getOutput() {
        return super.process(process(getInput()));
    }

    static class AddTimestampFn extends DoFn<Row, Row> {
        @ProcessElement
        public void processElement(@Element Row element, OutputReceiver<Row> receiver) {
            Long actionTime = element.getInt64("timestamp");

            Instant actionTimestamp = new Instant(actionTime);

            /*
             * Concept #2: Set the data element with that timestamp.
             */
            receiver.outputWithTimestamp(element, actionTimestamp);
        }
    }

}
