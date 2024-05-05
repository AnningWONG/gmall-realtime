package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunc;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsTradeProvinceOrderWindow
 * Package: com.atguigu.gmall.realtime.dws.app
 * Description:
 *      省份粒度下单业务过程聚合统计
 * @Author Wang Anning
 * @Create 2024/4/29 9:29
 * @Version 1.0
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(
                10020,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. 处理空消息，并对流中数据进行类型转换 jsonStr -> jsonObj，并过滤空消息
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (StringUtils.isNotEmpty(jsonStr)) {
                            JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // TODO 2. 去重
        // 2.1 按照唯一键（订单明细id）分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        // 2.2 去重
        // 利用状态去重
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        // 设置状态失效时间
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 从状态中获取这个订单明细ID的上条数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            // 如果状态中有数据，说明当前数据和状态中的数据重复，将状态中影响到度量值的字段取反，传递到下游
                            // 只有split_total_amount参与统计，仅对这个字段取反即可
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        // 所有的数据都更新状态，并向下游传递
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        // TODO 3. 指定水位线生成策略，提取事件时间字段
        SingleOutputStreamOperator<JSONObject> wmDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );
        // TODO 4. 再次对流中数据类型进行转换 jsonObj -> 用于统计的实体类对象
        // 将流中数据的order_id放到Set集合中
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = wmDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                        String orderId = jsonObj.getString("order_id");
                        Set<String> idSet = new HashSet<>();
                        idSet.add(orderId);
                        return TradeProvinceOrderBean.builder()
                                .provinceId(jsonObj.getString("province_id"))
                                // 字符串可以直接转化为BigDecimal
                                .orderAmount(jsonObj.getBigDecimal("split_total_amount"))
                                // Collections.singleton返回的是1个不可变集合
                                // .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                .orderIdSet(idSet)
                                .ts(jsonObj.getLong("ts") * 1000)
                                .build();
                    }
                }
        );
        // beanDS.print();
        // TODO 5. 按照统计的维度（省份）分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);
        // TODO 6. 开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowedDS = provinceIdKeyedDS.window(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        );
        // TODO 7. 聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedDS = windowedDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        // 将流中provinceId相同的每个数据的集合中的元素放到1个集合中，达到去重效果
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        // 补充窗口和日期相关属性值
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());

                        TradeProvinceOrderBean bean = input.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);
                        // Set集合的大小就是order数量
                        int orderCount = bean.getOrderIdSet().size();
                        bean.setOrderCount((long) orderCount);

                        out.collect(bean);

                    }
                }
        );
        // TODO 8. 关联省份维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceNameDS = AsyncDataStream.unorderedWait(
                reducedDS,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public void addDim(TradeProvinceOrderBean obj, JSONObject dimJsonObj) {
                        obj.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean obj) {
                        return obj.getProvinceId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withProvinceNameDS.print();
        // TODO 9. 写入Doris表中
        withProvinceNameDS.map(new BeanToJsonStrMapFunc<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));

    }
}
