package com.whpu.NetworkFlow_analysis;

import com.whpu.NetworkFlow_analysis.entry.ApacheLogEvent;
import com.whpu.NetworkFlow_analysis.entry.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * @author cc
 * @create 2020-09-10-15:59
 */
public class HotPagesNetworkFlow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设定时间语序 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度
        env.setParallelism(1);
        //获取数据源
        DataStream<String> dataStreamSource = env.readTextFile(env.getClass().getResource("/apache.log").getPath());
        DataStream<ApacheLogEvent> dataStream = dataStreamSource.map(new MapFunction<String, ApacheLogEvent>() {
            @Override
            public ApacheLogEvent map(String s) throws Exception {
                String[] str = s.split(" ");
                SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss");
                Long timesstamp = dateFormat.parse(str[3].trim()).getTime();
                ApacheLogEvent apacheLogEvent = new ApacheLogEvent(str[0].trim(), str[1].trim(), timesstamp, str[5].trim(), str[6].trim());
                //Long.valueOf(str[0].trim()),Long.valueOf(str[1].trim()),Integer.valueOf(str[2].trim()),str[3].trim(),Long.valueOf(str[4].trim())
                return apacheLogEvent;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(60)) {
            @Override //根据数据的情况设定一个60s的水位线
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.getTimestamp();
            }
        });
        //开窗聚合 以及排序输出；
        SingleOutputStreamOperator<PageViewCount> aggStream = dataStream
                .filter(apacheLogEvent -> apacheLogEvent.getMethod().equals("GET")).returns((Types.POJO(ApacheLogEvent.class)))
                .keyBy(new KeySelector<ApacheLogEvent, String>() {
                    @Override
                    public String getKey(ApacheLogEvent apacheLogEvent) throws Exception {
                        return apacheLogEvent.getUrl();
                    }
                })
                .timeWindow(Time.seconds(10),Time.seconds(5))
                .aggregate(new PageCountAgg(),new PageViewCountWindowResult());

        SingleOutputStreamOperator<String> process = aggStream.keyBy(new KeySelector<PageViewCount, Long>() {
            @Override
            public Long getKey(PageViewCount pageViewCount) throws Exception {
                return pageViewCount.getWindowEnd();
            }
        })
                .process(new TopNHotPages(5));
        process.print();
        env.execute();
    }

    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    private static class PageViewCountWindowResult implements WindowFunction<Long, PageViewCount,String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(key,window.getEnd(),input.iterator().next()));
        }
    }

    private static class TopNHotPages extends KeyedProcessFunction<Long,PageViewCount,String> {
        private int topSize;

        public TopNHotPages(int topSize) {
            this.topSize = topSize;
        }

        private ListState<PageViewCount> listState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //listState关联上下文
            ListStateDescriptor<PageViewCount> listStateDesc = new ListStateDescriptor<PageViewCount>("PageViewCount-state", PageViewCount.class);
            listState = getRuntimeContext().getListState(listStateDesc);
        }


        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            //将一个窗口内的数据装在listState中
            listState.add(value);
            //注册触发器
            //当waterMark到本窗口结束的下一秒 出发这个窗口的计算 也就是出发 onTimer（）
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 5 * 1000);
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //一旦出发了这个onTimer触发器 从listState中拿出所有数据 处理完 然后清空listState
            ArrayList<PageViewCount> list = new ArrayList<PageViewCount>();
            StringBuilder result = new StringBuilder();
            for (PageViewCount state:
                    listState.get()) {
                list.add(state);
            }
            //清空这个窗口的state
            listState.clear();
            //然后就是对这个arraylist输出排序 前topN
            Collections.sort(list, new Comparator<PageViewCount>() {
                @Override
                public int compare(PageViewCount o1, PageViewCount o2) {
                    if(o1.getCount() == o2.getCount()) {
                        return 0;
                    }else {
                        if(o1.getCount() < o2.getCount()) {
                            return 1;
                        }else {
                            return -1;
                        }
                    }
                }
            });

            result.append("====================\n");
            result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int j = 0; j < topSize; j++) {
                PageViewCount cur = list.get(j);
                // e.g.  No1：  商品ID=12224  浏览量=2413
                result.append("No").append(j + 1).append(":")
                        .append("  Url=").append(cur.getUrl())
                        .append("  流量=").append(cur.getCount()).append("\n");
            }
            result.append("=====================\n\n");
            Thread.sleep(1000);
            out.collect(result.toString());
        }
    }
}
