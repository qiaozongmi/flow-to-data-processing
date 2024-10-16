package com.ftdp.examples.processor;

import com.ftdp.processor.BaseProcessFunc;
import org.apache.beam.sdk.values.Row;

public class SimpleMapFunction extends BaseProcessFunc {
    public static Row processElement(Row row) {
        return row;
    }
}
