package com.ftdp.processor;

import org.apache.beam.sdk.values.Row;

public abstract class BaseProcessFunc implements ProcessFunc{
    public static Row processElement(Row row){
        row.getInt64("");
       return null;
   }
}
