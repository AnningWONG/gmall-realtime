package com.atguigu.gmall.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ClassName: TradeProvinceOrderCt
 * Package: com.atguigu.gmall.publisher.bean
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/4/29 15:23
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class TradeProvinceOrderCt {
    // 省份名称
    String provinceName;
    // 订单数
    Integer orderCt;
}

