package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.publisher.mapper.TradeStatsMapper;
import com.atguigu.gmall.publisher.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName: TradeStatsServiceImpl
 * Package: com.atguigu.gmall.publisher.service.impl
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/4/29 14:27
 * @Version 1.0
 */
//@Component
//@Controller
@Service
//@Repository
public class TradeStatsServiceImpl implements TradeStatsService {
    @Autowired
    private TradeStatsMapper tradeStatsMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatsMapper.selectProvinceAmount(date);
    }
}
