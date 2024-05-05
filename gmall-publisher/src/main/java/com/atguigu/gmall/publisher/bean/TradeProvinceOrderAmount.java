package com.atguigu.gmall.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ClassName: TradeProvinceOrderAmount
 * Package: com.atguigu.gmall.publisher.bean
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/4/29 17:59
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}

