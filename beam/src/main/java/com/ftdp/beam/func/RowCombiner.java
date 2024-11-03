package com.ftdp.beam.func;

import com.ftdp.beam.util.MergeRowUtil;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.Iterator;

public class RowCombiner extends Combine.CombineFn<Row, Row, Row> {
    Schema resultSchema;

    public RowCombiner(Schema resultSchema) {
        this.resultSchema = resultSchema;
    }

    @Override
    public Row createAccumulator() {
        return Row.withSchema(resultSchema).addValue(0L).build();
    }

    @Override
    public Row addInput(Row mutableAccumulator, Row input) {
        try {
            Row result = MergeRowUtil.mergeByMethod(input, mutableAccumulator, resultSchema);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row mergeAccumulators(@UnknownKeyFor @NonNull @Initialized Iterable<Row> accumulators) {
        Iterator<Row> itr = accumulators.iterator();
        Row result = createAccumulator();
        while (itr.hasNext()) {
            try {
                result = MergeRowUtil.mergeByMethod(result, itr.next(), resultSchema);
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
        return RowCoder.of(resultSchema);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Coder<Row> getDefaultOutputCoder(@UnknownKeyFor @NonNull @Initialized CoderRegistry registry, @UnknownKeyFor @NonNull @Initialized Coder<Row> inputCoder) throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException {
        return inputCoder;
    }
}
