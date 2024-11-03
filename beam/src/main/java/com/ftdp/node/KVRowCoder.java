//package com.ftdp.node;
//
//import org.apache.beam.sdk.coders.RowCoder;
//import org.apache.beam.sdk.schemas.Schema;
//import org.apache.beam.sdk.schemas.SchemaCoder;
//import org.apache.beam.sdk.transforms.SerializableFunctions;
//import org.apache.beam.sdk.values.KV;
//import org.apache.beam.sdk.values.Row;
//import org.apache.beam.sdk.values.TypeDescriptors;
//import org.checkerframework.checker.nullness.qual.Nullable;
//
//import java.util.Map;
//import java.util.Objects;
//import java.util.UUID;
//
//public class KVRowCoder extends SchemaCoder<KV<Row,Row>> {
//    public static KVRowCoder of(Schema schema) {
//        return new KVRowCoder(schema);
//    }
//
//    /** Override encoding positions for the given schema. */
//    public static void overrideEncodingPositions(UUID uuid, Map<String, Integer> encodingPositions) {
//        SchemaCoder.overrideEncodingPositions(uuid, encodingPositions);
//    }
//
//    private KVRowCoder(Schema schema) {
//        super(
//                schema,
//                TypeDescriptors.kvs(TypeDescriptors.rows(), TypeDescriptors.rows()),
//                SerializableFunctions.identity(),
//                SerializableFunctions.identity());
//    }
//
//    @Override
//    public boolean equals(@Nullable Object o) {
//        if (this == o) {
//            return true;
//        }
//        if (o == null || getClass() != o.getClass()) {
//            return false;
//        }
//        KVRowCoder rowCoder = (KVRowCoder) o;
//        return schema.equals(rowCoder.schema);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(schema);
//    }
//}
