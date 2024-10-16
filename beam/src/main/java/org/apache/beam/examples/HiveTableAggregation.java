package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hcatalog.HCatToRow;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.HashMap;
import java.util.Map;

public class HiveTableAggregation {
    public static void main(String[] args) {
        // 创建PipelineOptions
        PipelineOptions options = PipelineOptionsFactory.create();

        // 创建Pipeline
        Pipeline pipeline = Pipeline.create(options);

        // 从Hive表读取数据
        Map<String, String> configProperties = new HashMap<String, String>();
        configProperties.put("hive.metastore.uris","thrift://metastore-host:port");
        PCollection<Row> data = pipeline
                .apply(HCatToRow.fromSpec(HCatalogIO.read()
                        .withConfigProperties(configProperties)
                        .withDatabase("default") //optional, assumes default if none specified
                        .withTable("employee")
                        .withFilter("partition_time=20241001")));
        // 按照聚合维度分组并进行加法聚合
        PCollection<String> aggregatedData = data
                .apply(MapElements.via(
                        new SimpleFunction<Row, KV<Long,Long>>() {
                            @Override
                            public KV<Long,Long> apply(Row s) {
                                return KV.of(s.getInt64("aid"),s.getInt64("exp_pv"));
                            }
                        }))
                .apply(Combine.perKey(Sum.ofLongs()))
                .apply(MapElements.via(
                new SimpleFunction<KV<Long,Long>, String>() {
                    @Override
                    public String apply(KV<Long,Long> s) {
                        return s.toString();
                    }
                }));
        //                .apply(GroupByKey.create())
//                .apply(Combine.perKey(Sum.ofLongs()));

        // 输出聚合结果
        aggregatedData.apply("Write to output", TextIO.write().to("output.txt"));

        // 运行Pipeline
        pipeline.run().waitUntilFinish();
    }


}
