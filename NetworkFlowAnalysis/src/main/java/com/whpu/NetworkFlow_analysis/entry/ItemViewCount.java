package com.whpu.NetworkFlow_analysis.entry;

import lombok.Data;

/**
 * 设置窗口聚合函数样例类
 */
@Data
public class ItemViewCount{
    private Long itemId;
    private Long windowEnd;
    private Long count;

    public ItemViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public ItemViewCount() {
    }
}
