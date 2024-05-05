package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName: TradeStatsService
 * Package: com.atguigu.gmall.publisher.service
 * Description:
 *      交易域统计Service接口
 * @Author Wang Anning
 * @Create 2024/4/29 14:26
 * @Version 1.0
 */
public interface TradeStatsService {
    // 获取某天总交易额
    BigDecimal getGMV(Integer date);

    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
