package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * ClassName: TrafficChannelStatsMapper
 * Package: com.atguigu.gmall.publisher.mapper
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/4/29 20:03
 * @Version 1.0
 */
public interface TrafficChannelStatsMapper {
    // 获取某天不同渠道独立访客
    /*
    @Select("select ch,\n" +
            "\tsum(uv_ct) uv_ct\n" +
            "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
            "partition par#{param0}\n" +
            "group by ch\n" +
            "order by uv_ct desc" +
            "limit #{param1}")
    List<TrafficUvCt> selectUvCt(Integer date, Integer limit);
    */
    @Select("select ch,\n" +
            "\tsum(uv_ct) uv_ct\n" +
            "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
            "partition par#{date}\n" +
            "group by ch\n" +
            "order by uv_ct desc\n" +
            "limit #{limit}")
    List<TrafficUvCt> selectUvCt(@Param("date") Integer date, @Param("limit") Integer limit);
}
