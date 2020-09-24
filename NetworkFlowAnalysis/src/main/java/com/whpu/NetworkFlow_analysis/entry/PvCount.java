package com.whpu.NetworkFlow_analysis.entry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author cc
 * @create 2020-09-24-11:01
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PvCount {

    private long windowEnd;
    private long count;
}
