package com.whpu.NetworkFlow_analysis.entry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author cc
 * @create 2020-09-10-16:00
 *  输入数据的样例类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLogEvent {
    private String ip;
    private String userId;
    private Long  timestamp;
    private String method;
    private String url;

}
