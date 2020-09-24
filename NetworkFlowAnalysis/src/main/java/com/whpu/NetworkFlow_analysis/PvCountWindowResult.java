package com.whpu.NetworkFlow_analysis;

import com.whpu.NetworkFlow_analysis.entry.PvCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2020-09-24-11:16
 */
public class PvCountWindowResult implements WindowFunction<Long, PvCount, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> iterable, Collector<PvCount> collector) throws Exception {
        collector.collect(new PvCount(window.getEnd(),iterable.iterator().next().longValue()));
    }
}
