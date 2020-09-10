package com.whpu.hotitems_analysis;

import com.whpu.hotitems_analysis.entry.UserBehaivor;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author cc
 * @create 2020-09-08-15:49
 */
public class CountAgg implements AggregateFunction<UserBehaivor, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehaivor userBehaivor, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long acc2, Long acc1) {
            return acc1 + acc2;
        }
}
