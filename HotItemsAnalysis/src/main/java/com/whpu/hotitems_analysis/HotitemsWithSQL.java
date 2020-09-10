package com.whpu.hotitems_analysis;

import com.whpu.hotitems_analysis.entry.UserBehaivor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author cc
 * @create 2020-09-08-18:07
 * 问题报错记录二
 *      运行sql api时，使用函数No match found for function signature row_number()；
 *
 * 解决：
 *      由于只有阿里的blink api 中才有函数，所以要导入使用blink的jar包
 */
public class HotitemsWithSQL {
    public static void main(String[] args) throws Exception {
        //初始化上下文
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        //使用eventTime作为时间语义
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //获取数据源
        DataStream<String> dataStream = env.readTextFile(env.getClass().getResource("/UserBehavior.csv").getPath());
        DataStream<UserBehaivor> aggregateWindow = dataStream.map(new MapFunction<String, UserBehaivor>() {
            @Override
            public UserBehaivor map(String s) throws Exception {
                String[] str = s.split(",");
                UserBehaivor userBehaivor = new UserBehaivor(Long.valueOf(str[0].trim()), Long.valueOf(str[1].trim()), Integer.valueOf(str[2].trim()), str[3].trim(), Long.valueOf(str[4].trim()));
                //Long.valueOf(str[0].trim()),Long.valueOf(str[1].trim()),Integer.valueOf(str[2].trim()),str[3].trim(),Long.valueOf(str[4].trim())
                return userBehaivor;
            }
        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehaivor>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehaivor userBehaivor) {
                        return userBehaivor.getTimestamp() * 1000;
                    }
                });
//        aggregateWindow.print();
        //定义表执行环节
        Table dataTable = tableEnv.fromDataStream(aggregateWindow, "itemId,behavior,timestamp.rowTime");

        //table API 进行窗口聚合函数
        Table selectTable = dataTable.filter("behavior='pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("timestamp").as("slideWindow")) //设定滑动窗口的大小为一小时，每五分钟统计一次；
                .groupBy("itemId,slideWindow") //先按itemId分组再按照window分组
                .select("itemId,slideWindow.end as windowEnd, itemId.count as itemC");
        //使用sql去实现TopN的选取
        Table result = tableEnv.sqlQuery(
                "SELECT * FROM(" +
                        "SELECT *, " +
                        "row_number() " +
                        " OVER (partition by windowEnd order by itemC desc)" +
                        " as row_num " +
                        " FROM "+selectTable+
                        " )" +
                        "where row_num <= 5"
        );

        tableEnv.toRetractStream(result, Row.class)
                .print();
        env.execute("table api");

    }
}
