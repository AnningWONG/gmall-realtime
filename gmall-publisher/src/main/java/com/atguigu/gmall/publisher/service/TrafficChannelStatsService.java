package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.TrafficUvCt;

import java.util.List;

/**
 * ClassName: TrafficChannelStatsService
 * Package: com.atguigu.gmall.publisher.service
 * Description:
 *      流量域统计Service接口
 * @Author Wang Anning
 * @Create 2024/4/29 20:15
 * @Version 1.0
 */
public interface TrafficChannelStatsService {
    // 获取某天各渠道独立访客
    List<TrafficUvCt> getUvCt(Integer date, Integer limit);
}
