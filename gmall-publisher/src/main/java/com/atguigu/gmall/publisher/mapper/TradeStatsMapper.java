package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.bean.TradeProvinceOrderCt;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * ClassName: TradeStatsMapper
 * Package: com.atguigu.gmall.publisher.mapper
 * Description:
 *      交易域统计Mapper接口
 * @Author Wang Anning
 * @Create 2024/4/29 14:14
 * @Version 1.0
 */
public interface TradeStatsMapper {
    // 获取某天总交易额
    // 添加注解，底层封装JDBC实现
    // @Insert("")
    // @Delete("")
    // @Update("")
    @Select("select\n" +
            "\tsum(order_amount) order_amount\n" +
            "from dws_trade_province_order_window\n" +
            "partition par#{date}")
    BigDecimal selectGMV(Integer date);


    // 获取某天各个省份交易额
    @Select("select\n" +
            "\tprovince_name,\n" +
            "\tsum(order_amount) order_amount\n" +
            "from dws_trade_province_order_window\n" +
            "partition par#{date}\n" +
            "group by province_name")
    List<TradeProvinceOrderCt> selectProvinceAmount(Integer date);
}
/*
class MyC implements TradeStatsMapper{

    @Override
    public BigDecimal selectGMV(Integer date) {
        BigDecimal gmv;
        // 注册驱动
        try {
            Class.forName("xxx");
            // 获取连接
            Connection conn = DriverManager.getConnection("");
            // 获取数据库操作对象
            String sql = "select\n" +
                    "\tsum(order_amount) order_amount\n" +
                    "from dws_trade_province_order_window\n" +
                    "partition par20240429";
            PreparedStatement ps = conn.prepareStatement(sql);
            // 执行SQL
            ResultSet rs = ps.executeQuery();
            // 处理结果集
            rs.next();
            gmv = rs.getBigDecimal(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // 释放资源

        }
        return gmv;
    }
}
*/