package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.TrafficUvCt;
import com.atguigu.gmall.publisher.mapper.TrafficChannelStatsMapper;
import com.atguigu.gmall.publisher.service.TrafficChannelStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * ClassName: TrafficChannelStatsServiceImpl
 * Package: com.atguigu.gmall.publisher.service.impl
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/4/29 20:33
 * @Version 1.0
 */
@Service
public class TrafficChannelStatsServiceImpl implements TrafficChannelStatsService {
    @Autowired
    private TrafficChannelStatsMapper trafficChannelStatsMapper;
    @Override
    public List<TrafficUvCt> getUvCt(Integer date, Integer limit) {
        return trafficChannelStatsMapper.selectUvCt(date,limit);
    }
}
