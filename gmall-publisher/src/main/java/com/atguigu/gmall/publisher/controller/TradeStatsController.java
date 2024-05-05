package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.publisher.service.TradeStatsService;
import com.atguigu.gmall.publisher.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName: TradeStatsController
 * Package: com.atguigu.gmall.publisher.controller
 * Description:
 *      交易域统计Controller
 * @Author Wang Anning
 * @Create 2024/4/29 14:32
 * @Version 1.0
 */
// 将类对象创建和关系维护交给Spring
//@Controller // 如果方法返回值是String，会进行页面跳转；如果不想进行页面跳转，可以在方法上加@ResponseBody注解
@RestController // 如果方法返回值是String，不会进行页面跳转，而是将字符串直接返回给客户端
public class TradeStatsController {
    @Autowired
    private TradeStatsService tradeStatsService;
    // 底层维护Map集合，Key是请求路径，Value是方法签名
    @RequestMapping("/gmv")
    // 参数可以自动进行类型转换，通过注解可以设置默认值
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatsService.getGMV(date);
        String json = "{\n" +
                "  \"status\": 0,\n" +
                "  \"data\": " + gmv + "\n" +
                "}";
        return json;
    }
    /*
    @RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        // 调用Service方法，获取某天各省份交易额
        List<TradeProvinceOrderAmount> provinceAmountList = tradeStatsService.getProvinceAmount(date);


        // 拼串
        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceAmountList.size(); i++) {
            TradeProvinceOrderCt provinceAmount = provinceAmountList.get(i);
            jsonB.append("{\"name\": \"" + provinceAmount.getProvinceName() + "\",\"" + provinceAmount.getOrderCt() + "\": 5279}");
            if (i != provinceAmountList.size() - 1) {
                jsonB.append(",");
            }
        }
        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }
    */

    @RequestMapping("/province")
    public Map getProvinceAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        // 调用Service方法
        List<TradeProvinceOrderAmount> provinceAmountList = tradeStatsService.getProvinceAmount(date);


        // 面向对象
        Map resMap = new HashMap<>();
        resMap.put("status", 0);
        Map dataMap = new HashMap<>();
        List dataList = new ArrayList<>();
        for (TradeProvinceOrderAmount tradeProvinceOrderCt : provinceAmountList) {
            Map map = new HashMap<>();
            map.put("name", tradeProvinceOrderCt.getProvinceName());
            map.put("value", tradeProvinceOrderCt.getOrderAmount());
            dataList.add(map);
        }
        dataMap.put("mapData",dataList);
        dataMap.put("valueName","交易额");
        resMap.put("data", dataMap);
        return resMap;

    }

}
