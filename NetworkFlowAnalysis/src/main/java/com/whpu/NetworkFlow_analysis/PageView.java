package com.whpu.NetworkFlow_analysis;

import com.whpu.NetworkFlow_analysis.entry.ApacheLogEvent;
import com.whpu.NetworkFlow_analysis.entry.PvCount;
import com.whpu.NetworkFlow_analysis.entry.UserBehaivor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2020-09-24-10:34
 */
public class PageView {
    public static void main(String[] args) throws Exception {
        //引入Flink执行环境，设置时间语序及并行度
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //source-读取本地的数据源 当前类所在的包环境，找到名字，然后getPath拿出路径；
        DataStream<String> dataSource = env.readTextFile(env.getClass().getResource("/UserBehavior.csv").getPath());
        //转换成样例类类型，并提取时间戳和watermark
        SingleOutputStreamOperator<UserBehaivor> dataStream = dataSource.map(new MapFunction<String, UserBehaivor>() {
            @Override
            public UserBehaivor map(String s) throws Exception {
                String[] str = s.split(",");
                UserBehaivor userBehaivor = new UserBehaivor(Long.valueOf(str[0].trim()), Long.valueOf(str[1].trim()), Integer.valueOf(str[2].trim()), str[3].trim(), Long.valueOf(str[4].trim()));
                return userBehaivor;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehaivor>() {
            @Override
            public long extractAscendingTimestamp(UserBehaivor userBehaivor) {
                return userBehaivor.getTimestamp()*1000;
            }
        });
        SingleOutputStreamOperator<PvCount> aggregate = dataStream
                .filter(userBehaivor -> userBehaivor.getBehavior().equals("pv") ).returns(Types.POJO(UserBehaivor.class))
                .map(userBehaivor ->  Tuple2.of(userBehaivor.getBehavior(), 1L)).returns(Types.TUPLE(TypeInformation.of(String.class),TypeInformation.of(Long.class)))
                .keyBy(tuple2 -> tuple2.f0)
                .timeWindow(Time.hours(1))
                .allowedLateness(Time.seconds(60))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Tuple2<String, Long> tuple2, Long aLong) {
                        return aLong + 1;
                    }

                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return acc1 + aLong;
                    }
                }, new WindowFunction<Long, PvCount, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Long> iterable, Collector<PvCount> collector) throws Exception {
                        collector.collect(new PvCount(window.getEnd(),iterable.iterator().next().longValue()));
                    }
                });//new PvCountAgg(), new PvCountWindowResult());
        aggregate.print("aggregate");
        env.execute();
    }
}
