package com.ftdp.beam.func;

import com.ftdp.beam.util.MergeRowUtil;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.io.*;
import java.util.Iterator;
import java.util.stream.StreamSupport;

public class RowCombiner extends Combine.CombineFn<Row, Row, Row> {
    @Override
    public Row createAccumulator() {
        return Row.nullRow(Schema.builder().build());
    }

    @Override
    public Row addInput(Row mutableAccumulator, Row input) {
        try {
            return MergeRowUtil.mergeByMethod(input, mutableAccumulator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row mergeAccumulators(@UnknownKeyFor @NonNull @Initialized Iterable<Row> accumulators) {
        Iterator<Row> itr = accumulators.iterator();
        Row result = Row.nullRow(Schema.builder().build());
        if (itr.hasNext()) {
            try {
                MergeRowUtil.mergeByMethod(result, itr.next());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    @Override
    public Row extractOutput(Row accumulator) {
        return accumulator;
    }
    @Override
    public Coder<Row> getAccumulatorCoder(CoderRegistry registry, Coder<Row> inputCoder) {
        return new AtomicCoder<Row>() {
            @Override
            public void encode(Row value, OutputStream outStream) throws IOException {
                VarInt.encode(value, outStream);
            }

            @Override
            public Row decode(InputStream inStream) throws IOException, CoderException {
                try {
                    return new long[] {VarInt.decodeLong(inStream)};
                } catch (EOFException | UTFDataFormatException exn) {
                    throw new CoderException(exn);
                }
            }

            @Override
            public boolean isRegisterByteSizeObserverCheap(Row value) {
                return true;
            }

            @Override
            protected long getEncodedElementByteSize(Row value) {
                return VarInt.getLength(value[0]);
            }
        };
    }
}
