package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunc;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.eclipse.jetty.util.StringUtil;

/**
 * ClassName: DwsTradeCartAddUuWindow
 * Package: com.atguigu.gmall.realtime.dws.app
 * Description:
 *      加购独立用户统计
 * @Author Wang Anning
 * @Create 2024/4/25 16:08
 * @Version 1.0
 */
public class DwsTradeCartAddUuWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(
                10026,
                4,
                "dws_trade_cart_add_uu_window",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. 对流中数据进行转化 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        // jsonObjDS.print();
        // TODO 2. 指定水位线和提取事件时间字段
        SingleOutputStreamOperator<JSONObject> wmDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        // ts字段是由binlog生成的，单位是秒
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );
        // TODO 3. 按照用户ID分组
        KeyedStream<JSONObject, String> keyedDS = wmDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
        // TODO 4. Flink状态编程，判断是否为加购独立用户，直接将这条jsonObj向下游传递
        SingleOutputStreamOperator<JSONObject> processedDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 每个用户的状态中保存他上一次加购的日期
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastCartDateState", String.class);
                        // 状态失效时间为1天
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj,
                                               KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        // 获取当天日期和上次加购日期
                        Long ts = jsonObj.getLong("ts") * 1000;
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        String lastCartDate = lastCartDateState.value();
                        // 如果上次加购日期为空，或上次加购日期和当天日期不一致，说明该用户为当天新增加购用户
                        // 直接向下游传递并更新状态
                        if (StringUtil.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );
        // processedDS.print();
        // TODO 5. 开窗
        // 10s 滚动事件时间窗口
        AllWindowedStream<JSONObject, TimeWindow> windowedDS = processedDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        // TODO 6. 聚合
        // 输入，累加器和输出数据类型不一致，用aggregate算子
        // 传入增量聚合函数对象和全量聚合函数对象
        // 其中增量聚合函数为AggregateFunction，全量聚合函数为AllWindowFunction
        SingleOutputStreamOperator<CartAddUuBean> aggDS = windowedDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    // 初始化累加器
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }
                    // 每来1条数据，累加器加1，前加加（先自增再返回）
                    @Override
                    public Long add(JSONObject jsonObj, Long accumulator) {
                        return ++accumulator;
                    }
                    // 直接把累加器返回
                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }
                    // 不会用到的方法
                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                // 封装实体Bean
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        Long cartUuCt = values.iterator().next();
                        out.collect(
                                new CartAddUuBean(
                                        stt,
                                        edt,
                                        curDate,
                                        cartUuCt
                                )
                        );
                    }
                }
        );
        aggDS.print();
        // TODO 7. 将聚合结果写入Doris
        aggDS.map(new BeanToJsonStrMapFunc<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));
    }
}
