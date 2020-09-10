package com.whpu.hotitems_analysis;

import com.whpu.hotitems_analysis.entry.ItemViewCount;
import com.whpu.hotitems_analysis.entry.UserBehaivor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author cc
 * @create 2020-09-04-14:29
 */
public class Hotitems {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //设定时间语序 EventTime
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            //设置并行度
            env.setParallelism(1);
            //获取数据源
            DataStream<String> dataStreamSource = env.readTextFile(env.getClass().getResource("/UserBehavior.csv").getPath());
            DataStream<ItemViewCount> aggregateWindow = dataStreamSource.map(new MapFunction<String, UserBehaivor>() {
                @Override
                public UserBehaivor map(String s) throws Exception {
                    String[] str = s.split(",");
                    UserBehaivor userBehaivor = new UserBehaivor(Long.valueOf(str[0].trim()), Long.valueOf(str[1].trim()), Integer.valueOf(str[2].trim()), str[3].trim(), Long.valueOf(str[4].trim()));
                    //Long.valueOf(str[0].trim()),Long.valueOf(str[1].trim()),Integer.valueOf(str[2].trim()),str[3].trim(),Long.valueOf(str[4].trim())
                    return userBehaivor;
                }
            }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehaivor>() {
            @Override
            public long extractAscendingTimestamp(UserBehaivor userBehaivor) {
                return userBehaivor.getTimestamp()*1000;
            }
        }).filter(userBehaivor ->  userBehaivor.getBehavior().equals("pv")).returns(Types.GENERIC(UserBehaivor.class)) //过滤pv行为
                        .keyBy(new KeySelector<UserBehaivor, Long>() {
                    @Override
                    public Long getKey(UserBehaivor userBehaivor) throws Exception {
                        return userBehaivor.getItemId();
                    }
                })//按照商品的id进行统计
                .timeWindow(Time.seconds(60), Time.seconds(5)) //设置滑动窗口进行计算
                .aggregate(new CountAgg(), new WindowResultFunction());
        //得到窗口聚合的结果

        aggregateWindow.keyBy(new KeySelector<ItemViewCount, Long>() {
            @Override
            public Long getKey(ItemViewCount itemViewCount) throws Exception {
                return itemViewCount.getWindowEnd();
            }
        }) //按照窗口分组，收集当前窗口内的商品count数据
                .process(new TopNHotItems(5))//自定义处理流程
                .print();
//        aggregateWindow.print();
        env.execute();

    }
}

