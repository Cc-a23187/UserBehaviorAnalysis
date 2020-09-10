package com.whpu.NetworkFlow_analysis.entry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author cc
 * @create 2020-09-10-16:01
 * 窗口聚合结果样例类
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageViewCount {
    private String url;
    private Long windowEnd;
    private Long count;

}
