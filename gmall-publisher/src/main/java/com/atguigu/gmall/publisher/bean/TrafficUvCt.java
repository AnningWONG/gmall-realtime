package com.atguigu.gmall.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ClassName: TrafficUvCt
 * Package: com.atguigu.gmall.publisher.bean
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/4/29 20:02
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}

