package com.whpu.NetworkFlow_analysis.entry;

import lombok.Data;

/**
 * 设置输入数据样例类
 *
 * 问题报错记录；
 * 报错内容：
 *      //org.apache.flink.table.api.ValidationException: Too many fields referenced from an atomic type.
 *      //这个无参构造方法必须要有，要不会报错...参考：https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/api_concepts.html#pojo
 *
 * 解决：
 *      需要在pojo实体类中加入无参构造周期爱你写了lombok.AllArgsConstructor的方法没有生成无参构造；
 *
 * 写在后面
 *  	lombok.AllArgsConstructor顾名思义为全参的构造函数
 *      lombok.NoArgsConstructor为无参构造
 */
@Data
public class UserBehaivor{
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public UserBehaivor() {
    }

    public UserBehaivor(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
