package com.whpu.NetworkFlow_analysis;

import com.whpu.NetworkFlow_analysis.entry.PvCount;
import com.whpu.NetworkFlow_analysis.entry.UserBehaivor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import static java.awt.SystemColor.window;

/**
 * @author cc
 * @create 2020-09-24-11:16
 */
public class PvCountAgg implements AggregateFunction<UserBehaivor,Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehaivor userBehaivor, Long aLong) {
        return aLong+1;
    }



    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long acc2, Long acc1) {
        return acc1+acc2;
    }
}
