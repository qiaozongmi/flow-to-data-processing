package com.ftdp.beam.util;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MergeRowUtil {
    public static Row mergeByMethod(Row orgRow, Row newRow) throws Exception {
        List<Object> valueList = new ArrayList<>();
        Schema.Builder valueSchemaBuilder = Schema.builder();
        Schema schema;
        int orgCnt = orgRow.getFieldCount();
        int newCnt = newRow.getFieldCount();
        if (orgCnt > newCnt) {
            schema = orgRow.getSchema();
        } else {
            schema = newRow.getSchema();
        }
        int size = schema.getFieldCount();

        for (int i = 0; i < size; i++) {
            Schema.Field field = schema.getField(i);
            String method = field.getOptions().getValueOrDefault("reduce_method", "");
            String fieldName = field.getOptions().getValueOrDefault("alias", field.getName());

            Object mergeVal;

            switch (method) {
                case "sum":
                    switch (field.getType().getTypeName()) {
                        case INT16:
                        case INT32:
                        case INT64:
                        case DOUBLE:
                        case FLOAT:
                            mergeVal = orgRow.getInt32(i) + newRow.getInt32(i);
                            break;
                        default:
                            throw new Exception("unsurpport type" + field.getType().getTypeName() + " for reduce_method sum");
                    }
                    break;
                default:
                    throw new Exception("no reduce_method conf found");
            }
            field = field.withName(fieldName);
            valueSchemaBuilder.addField(field);
            valueList.add(mergeVal);
        }
        return Row.withSchema(valueSchemaBuilder.build()).addValues(valueList).build();
    }

    public static Row mergeBaseOneRow(Row orgRow, Row newRow) {
        Schema orgSchema = orgRow.getSchema();
        Schema newSchema = newRow.getSchema();

        List<Object> valueList = new ArrayList<>();
        Schema.Builder schema = Schema.builder();

        for (int i = 0; i < orgSchema.getFieldCount(); i++) {
            valueList.add(orgRow.getValue(i));
            schema.addField(orgSchema.getField(i));
        }
        for (int i = 0; i < newSchema.getFieldCount(); i++) {
            valueList.add(newRow.getValue(i));
            schema.addField(newSchema.getField(i));
        }
        return Row.withSchema(schema.build()).addValues(valueList).build();
    }

    public static Row mergeOnAlias(Row... rows) {
        List<Object> valueList = new ArrayList<>();
        Schema.Builder schema = Schema.builder();

        Arrays.stream(rows).forEach(
                row -> {
                    if(row != null){
                        Schema orgSchema = row.getSchema();
                        for (int i = 0; i < orgSchema.getFieldCount(); i++) {
                            valueList.add(row.getValue(i));
                            Schema.Field field = orgSchema.getField(i);
                            field = field.withName(field.getOptions().getValueOrDefault("alias", field.getName()));
                            schema.addField(field);
                        }
                    }
                });
        return Row.withSchema(schema.build()).addValues(valueList).build();
    }
}
