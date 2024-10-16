package com.ftdp.processor;

import org.apache.beam.sdk.values.Row;

public interface ProcessFunc {
    public static Row processElement(Row row) {
        return null;
    }
}
