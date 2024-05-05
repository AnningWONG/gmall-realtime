package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunc;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ClassName: DwsUserUserLoginWindow
 * Package: com.atguigu.gmall.realtime.dws.app
 * Description:
 *      独立用户以及回流用户统计
 * @Author Wang Anning
 * @Create 2024/4/25 15:19
 * @Version 1.0
 */
public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(
                10024,
                4,
                "dws_user_user_login_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. 对流中数据进行类型转化 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        // TODO 2. 过滤登录行为
        // uid不为空，上一页为login或为空
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String uid = jsonObj.getJSONObject("common").getString("uid");
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return StringUtils.isNotEmpty(uid)
                                && ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
                    }
                }
        );
        // filterDS.print();
        // TODO 3. 指定水位线提取事件时间字段
        // 水位线生成策略：单调递增，事件时间字段是时间戳
        SingleOutputStreamOperator<JSONObject> wmDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );
        // TODO 4. 按照uid进行分组
        KeyedStream<JSONObject, String> keyedDS = wmDS
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"));
        // TODO 5. Flink状态编程，判断是否独立用户和回流用户，并封装统计的实体类对象
        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                    // 状态封装这个用户上一次登录的日期
                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastLoginDateState", String.class);
                        // 状态不失效，不设定失效时间
                        lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                        // 获取当日日期
                        Long ts = jsonObj.getLong("ts");
                        String curLoginDate = DateFormatUtil.tsToDate(ts);
                        // 状态中获取上次登录日期
                        String lastLoginDate = lastLoginDateState.value();
                        Long uuCt = 0L;
                        Long backCt = 0L;
                        // 状态中上次登录日期不为空
                        if (StringUtils.isNotEmpty(lastLoginDate)) {
                            // 状态中上次登录日期也不是当日，是当日新用户
                            if (!lastLoginDate.equals(curLoginDate)) {
                                // uuCt为1，当日日期更新到状态中
                                uuCt = 1L;
                                lastLoginDateState.update(curLoginDate);
                                // 如果是回流用户，backCt为1
                                Long days = (ts - DateFormatUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                                if (days > 7) {
                                    backCt = 1L;
                                }
                            }
                        // 状态中上次登录日期为空，是网站的新用户
                        } else {
                            // uuCt为1，当日日期更新到状态中
                            uuCt = 1L;
                            lastLoginDateState.update(curLoginDate);
                        }
                        // 如果是新用户或回流用户才将数据封装后向下游传递
                        if (uuCt != 0L || backCt != 0L) {
                            out.collect(
                                    new UserLoginBean(
                                            "", "", "", uuCt, backCt, ts
                                    )
                            );
                        }
                    }
                }
        );
        // beanDS.print();
        // TODO 6. 开窗
        // 10s滚动事件时间窗口
        AllWindowedStream<UserLoginBean, TimeWindow> windowedDS = beanDS.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );
        // TODO 7. 聚合
        SingleOutputStreamOperator<UserLoginBean> reducedDS = windowedDS.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1,
                                                UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window,
                                      Iterable<UserLoginBean> values,
                                      Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean bean = values.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));

                        out.collect(bean);
                    }
                }
        );
        reducedDS.print();

        // TODO 8. 写入Doris
        reducedDS.map(new BeanToJsonStrMapFunc<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));

    }
}
