package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.bean.TrafficUvCt;
import com.atguigu.gmall.publisher.service.TrafficChannelStatsService;
import com.atguigu.gmall.publisher.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName: TrafficController
 * Package: com.atguigu.gmall.publisher.controller
 * Description:
 *      流量域统计Controller
 * @Author Wang Anning
 * @Create 2024/4/29 20:36
 * @Version 1.0
 */
@RestController
public class TrafficController {
    @Autowired
    private TrafficChannelStatsService trafficChannelStatsService;

    @RequestMapping("/uvCt")
    public String getUvCt(@RequestParam(value = "date", defaultValue = "0") Integer date,
                          @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }

        List<TrafficUvCt> trafficUvCtList = trafficChannelStatsService.getUvCt(date,limit);
        /*
        StringBuilder jsonB1 = new StringBuilder("{\"status\": 0,\"data\": {\"categories\": [");
        StringBuilder jsonB2 = new StringBuilder("],\"series\": [{\"name\": \"独立访客数\",\"data\": [");
        for (int i = 0; i < trafficUvCtList.size(); i++) {
            TrafficUvCt trafficUvCt = trafficUvCtList.get(i);
            jsonB1.append("\"" + trafficUvCt.getCh() + "\"");
            jsonB2.append(trafficUvCt.getUvCt());
            if (i != trafficUvCtList.size() - 1) {
                jsonB1.append(",");
                jsonB2.append(",");
            }
        }
        jsonB1.append(jsonB2).append("]}]}}");
        return jsonB1.toString();
        */
        ArrayList<Object> chList = new ArrayList<>();
        ArrayList<Object> uvCtList = new ArrayList<>();
        for (TrafficUvCt trafficUvCt : trafficUvCtList) {
            chList.add(trafficUvCt.getCh());
            uvCtList.add(trafficUvCt.getUvCt());
        }
        ;
        String json = "{\"status\": 0,\"data\": {\"categories\": [\"" + StringUtils.join(chList,"\",\"")
                + "\"],\"series\": [{\"name\": \"独立访客数\",\"data\": [\"" + StringUtils.join(uvCtList,"\",\"")
                + "\"]}]}}";
        return json;
    }

    /*
    @RequestMapping("/uvCt")
    public Map getUvCt(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficChannelStatsService.getUvCt(date);
        Map resMap = new HashMap<>();
        resMap.put("status",0);
        List categories = new ArrayList<>();
        List data = new ArrayList<>();
        for (TrafficUvCt trafficUvCt : trafficUvCtList) {
            categories.add(trafficUvCt.getCh());
            data.add(trafficUvCt.getUvCt());
        }
        Map series1 = new HashMap<>();
        series1.put("name","独立访客数");
        series1.put("data",data);
        List series = new ArrayList<>();
        series.add(series1);
        Map dataOut = new HashMap<>();
        dataOut.put("categories",categories);
        dataOut.put("series",series);
        resMap.put("data", dataOut);
        return resMap;
    }
    */
}
