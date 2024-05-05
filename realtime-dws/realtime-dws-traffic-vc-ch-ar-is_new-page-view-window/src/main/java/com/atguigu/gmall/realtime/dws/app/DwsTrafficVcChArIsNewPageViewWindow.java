package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunc;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * ClassName: DwsTrafficVcChArIsNewPageViewWindow
 * Package: com.atguigu.gmall.realtime.dws.app
 * Description:
 *      版本，渠道，地区，新老访客聚合统计
 *      度量：sv,pv,uv,dur
 * @Author Wang Anning
 * @Create 2024/4/24 15:44
 * @Version 1.0
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,
                4,
                "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. 对当前流中数据进行类型转化 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);


        // TODO 2. 按照设备ID进行分组，统计uv
        KeyedStream<JSONObject, String> keyedDS =
                jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));


        // TODO 3. 对分组后的数据进行处理 jsonObj -> 用于统计的实体类对象
        SingleOutputStreamOperator<TrafficPageViewBean> viewDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
                    // 键控状态保存上次访问日期
                    private ValueState<String> lastVisitDateState;

                    // open方法可以获得运行时上下文，可以对状态初始化
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 状态描述器
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        // 状态只保留1天
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        // 通过上下文对象对状态初始化
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj,
                                               KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context ctx,
                                               Collector<TrafficPageViewBean> out) throws Exception {
                        JSONObject common = jsonObj.getJSONObject("common");
                        JSONObject page = jsonObj.getJSONObject("page");
                        // 获取度量字段
                        String vc = common.getString("vc");
                        String ch = common.getString("ch");
                        String ar = common.getString("ar");
                        String isNew = common.getString("is_new");
                        // 根据是否有last_page_id判断sv计数为0还是1，只有第1个page记为1
                        String lastPageId = page.getString("last_page_id");
                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;
                        // 获取时间戳，进而获取格式为yyyy-MM-dd的日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        // 从状态中获取上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        Long uvCt = 0L;
                        // 如果状态中当前设备ID的上次访问日期是空，或者跨越天但状态尚未过期的情况，都要将uv记为1，并更新状态
                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);
                        }
                        // 根据数据中的信息封装对象，其中窗口开始和结束时间需要开窗后获取，cur_date根据窗口时间获取更准确
                        TrafficPageViewBean viewBean = new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                vc,
                                ch,
                                ar,
                                isNew,
                                uvCt,
                                svCt,
                                1L, // 1条数据就是1个pv
                                page.getLong("during_time"),
                                ts
                        );
                        out.collect(viewBean);
                    }
                }
        );
        viewDS.print();

        // TODO 4. 指定watermark生成策略，提取事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> wmDS = viewDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // 单调递增
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean view, long recordTimestamp) {
                                        // 数据中的时间戳和水位线的时间戳都是毫秒
                                        return view.getTs();
                                    }
                                }
                        )
        );
        // TODO 5. 按照统计维度进行分组
        // 统计维度被封装为1个Tuple4
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = wmDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {

                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean view) throws Exception {
                        return Tuple4.of(
                                view.getVc(),
                                view.getCh(),
                                view.getAr(),
                                view.getIsNew()
                        );
                    }
                }
        );
        // TODO 6. 开窗
        // 10s 滚动事件时间窗口
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedDS =
                dimKeyedDS.window(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        );
        // TODO 7. 聚合计算
        // reduce和aggregate算子都可以传两个函数对象（增量聚合函数对象和全量聚合函数对象）
        // 输入，输出和累加器数据类型都一样，用reduce算子
        // 增量聚合函数选ReduceFunction，全量聚合函数选WindowFunction
        SingleOutputStreamOperator<TrafficPageViewBean> reducedDS = windowedDS.reduce(
                // 聚合逻辑
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1,
                                                      TrafficPageViewBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                // 向数据中补充窗口起止时间和日期字段，日期字段值和窗口统一
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4,
                                      TimeWindow window,
                                      Iterable<TrafficPageViewBean> input,
                                      Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean view = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        view.setStt(stt);
                        view.setEdt(edt);
                        view.setCur_date(curDate);
                        out.collect(view);
                    }
                }
        );
        reducedDS.print();
        // TODO 8. 将聚合结果写到Doris中
        reducedDS
                .map(new BeanToJsonStrMapFunc<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }
}
