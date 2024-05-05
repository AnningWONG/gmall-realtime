package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
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
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ClassName: DwsTrafficHomeDetailPageViewWindow
 * Package: com.atguigu.gmall.realtime.dws.app
 * Description:
 *      首页，详情页独立访客
 * @Author Wang Anning
 * @Create 2024/4/25 14:02
 * @Version 1.0
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(
                10023,
                4,
                "dws_traffic_home_detail_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. 将流中数据进行类型转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        // TODO 2. 过滤出首页和详情页
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                }
        );
        // filterDS.print();
        // TODO 3. 指定水位线，提取事件时间字段
        // 水位线生成策略：单调递增；事件时间为数据中的时间戳
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
        // TODO 4. 按mid分组
        KeyedStream<JSONObject, String> keyedDS = wmDS
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        // TODO 5. 使用Flink状态编程判断是否为首页和详情页独立访客，并封装为统计的实体类对象
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> viewDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    // 每个独立访客2个值状态，分别是上一次浏览首页的日期和上一次浏览详情页的日期
                    private ValueState<String> homeLastVisitDateState;
                    private ValueState<String> goodDetailLastVisitDateState;
                    // open方法中对状态初始化
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> homeStateDescriptor
                                = new ValueStateDescriptor<>("homeLastVisitDateState", String.class);
                        homeLastVisitDateState = getRuntimeContext().getState(homeStateDescriptor);

                        ValueStateDescriptor<String> goodDetailStateDescriptor
                                = new ValueStateDescriptor<>("goodDetailLastVisitDateState", String.class);
                        goodDetailLastVisitDateState = getRuntimeContext().getState(goodDetailStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        // 获取时间戳
                        Long ts = jsonObj.getLong("ts");
                        // 转换为yyyy-MM-dd格式
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        Long homeUvCnt = 0L;
                        Long goodDetailUvCnt = 0L;
                        // 首页和详情页分别处理
                        // 如果状态中没有数据或数据已经是curVisitDate之前的，给这条数据标记为1，并用curVisitDate更新状态
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        if ("home".equals(pageId)) {
                            String homeLastVisitDate = homeLastVisitDateState.value();
                            if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                                homeUvCnt = 1L;
                                homeLastVisitDateState.update(curVisitDate);
                            }
                        } else {
                            String goodDetailLastVisitDate = goodDetailLastVisitDateState.value();
                            if (StringUtils.isEmpty(goodDetailLastVisitDate) || !goodDetailLastVisitDate.equals(curVisitDate)) {
                                goodDetailUvCnt = 1L;
                                goodDetailLastVisitDateState.update(curVisitDate);
                            }
                        }
                        // 首页或详情页的独立访客才向下游传递
                        if (homeUvCnt != 0L || goodDetailUvCnt != 0L) {
                            out.collect(
                                    new TrafficHomeDetailPageViewBean(
                                            "", "", "", homeUvCnt, goodDetailUvCnt, ts
                                    )
                            );
                        }
                    }
                }
        );
        // viewDS.print();
        // TODO 6. 开窗
        // 10s的滚动事件时间窗口
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> wsDS = viewDS.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );
        // TODO 7. 聚合
        // reduce和aggregate算子都可以传两个函数对象（增量聚合函数对象和全量聚合函数对象）
        // 输入，输出和累加器数据类型都一样，用reduce算子
        // 增量聚合函数选ReduceFunction，全量聚合函数选ProcessAllWindowFunction
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reducedDS = wsDS.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    // 聚合逻辑
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    // 向数据中补充窗口起止时间和日期字段，日期字段值和窗口统一
                    @Override
                    public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDate = DateFormatUtil.tsToDate(context.window().getStart());
                        TrafficHomeDetailPageViewBean view = elements.iterator().next();
                        view.setStt(stt);
                        view.setEdt(edt);
                        view.setCurDate(curDate);
                        out.collect(view);
                    }
                }
        );
        // reducedDS.print();
        // TODO 8. 将聚合结果写到Doris中
        reducedDS
                .map(new BeanToJsonStrMapFunc<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));
    }
}
