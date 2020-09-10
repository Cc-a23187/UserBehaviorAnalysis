package com.whpu.hotitems_analysis;

import com.whpu.hotitems_analysis.entry.ItemViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2020-09-08-15:50
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
    @Override
    public void apply(Long tuple, TimeWindow window, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
//        Long itemId = ((Tuple1<Long>) tuple).f0;
        Long itemId = tuple;
        Long end = window.getEnd();
        Long count = iterable.iterator().next();
        collector.collect(new ItemViewCount(itemId, end, count));
    }

}
