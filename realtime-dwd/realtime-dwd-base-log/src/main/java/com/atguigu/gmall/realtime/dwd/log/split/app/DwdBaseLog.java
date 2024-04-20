package com.atguigu.gmall.realtime.dwd.log.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * ClassName: DwdBaseLog
 * Package: com.atguigu.gmall.realtime.dwd.log.split.app
 * Description:
 *      需要启动ZooKeeper，Kafka，Flume，HDFS（检查点存储位置）
 * @Author Wang Anning
 * @Create 2024/4/19 9:41
 * @Version 1.0
 */
public class DwdBaseLog extends BaseApp {

    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";


    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. 对读取到的流中数据进行类型转换和简单ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        // TODO 2. 使用Flink状态编程修复新老访客标记
        SingleOutputStreamOperator<JSONObject> fixedDS = fixNewOld(jsonObjDS);

        // TODO 3. 按照日志的类型进行分流操作
        Map<String, DataStream> dsMap = splitStream(fixedDS);

        // TODO 4. 将不同流数据写入Kafka不同主题中
        dsMap.get(ERR).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        dsMap.get(START).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        dsMap.get(DISPLAY).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        dsMap.get(ACTION).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        dsMap.get(PAGE).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));

    }

    private Map<String, DataStream> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        // 错误日志 -> 错误侧输出流，启动日志 -> 启动侧输出流，曝光日志 -> 曝光侧输出流，动作日志 -> 动作侧输出流，页面日志 -> 主流
        // 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        // 分流，只能用process算子，因为只有上下文对象才能将数据放到侧输出流中，而只有process算子才提供上下文对象
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        // 处理错误日志
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            // 将错误日志写到错误侧输出流
                            // JSONObject的toString()底层调用toJSONString()，堆栈次数多一次，所以用toJSONString()
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            // 处理启动日志
                            // 将启动日志写到启动侧输出流
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            // 处理页面日志
                            // 获取页面日志中都应该有的3个属性，common，page，ts
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
                            // 处理页面日志中的曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                // 遍历曝光数组，获取每一条曝光数据
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    // 定义新的json对象，用于封装曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    // 将曝光数据写到曝光侧输出流
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }
                            // 处理页面日志中的动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    newActionJsonObj.put("ts", ts);
                                    // 将动作数据写到动作侧输出流
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
                            // 将页面日志写到主流中
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        // errDS.print("错误：");
        // startDS.print("启动：");
        // displayDS.print("曝光：");
        // actionDS.print("动作：");
        // pageDS.print("页面：");

        Map<String, DataStream> dsMap = new HashMap<>();
        dsMap.put(ERR, errDS);
        dsMap.put(START, startDS);
        dsMap.put(DISPLAY, displayDS);
        dsMap.put(ACTION, actionDS);
        dsMap.put(PAGE, pageDS);
        return dsMap;
    }

    private SingleOutputStreamOperator<JSONObject> fixNewOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        // 按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        // 修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDataState;
                    // Function的属性如果在声明时初始化，随Function对象创建
                    // private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 创建状态描述器
                        ValueStateDescriptor<String> lastVisitDescriptor
                                = new ValueStateDescriptor<>("lastVisitDescriptor", String.class);
                        // 为状态初始化
                        lastVisitDataState = getRuntimeContext().getState(lastVisitDescriptor);
                        // Function的属性如果在open方法中初始化，随算子创建
                        // sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 获取状态值
                        String lastVisitDate = lastVisitDataState.value();
                        // 从日志数据中获取时间戳
                        Long ts = jsonObj.getLong("ts");
                        // 自己封装日期处理工具类，将时间戳转日期
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        // 获取is_new的值
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        // 先判断日志数据中的is_new是否为1
                        // 如果is_new为1
                        if ("1".equals(isNew)) {
                            // 如果状态为空，就用这条数据的时间为状态初始化
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDataState.update(curVisitDate);
                                // 如果状态不为空，说明这条数据的is_new是错误的
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                            // 如果is_new为0
                        } else {
                            // 如果状态为空，数仓刚上线，状态不全导致老用户首日日期不在状态里，将日志数据中当天之前的日期放到状态里
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDataState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000));
                            }
                        }
                        out.collect(jsonObj);
                    }
                }
        );
        // fixedDS.print();

        return fixedDS;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        // 定义侧输出流标签，用于标记脏数据
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        // ETL，脏数据输出到侧流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            // jsonStr -> jsonObj
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            // 如果转换过程中，没有发生异常，说明是标准json，直接将其向下游传递，传递的是jsonObj
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            // 如果转换过程中，发生了异常，说明不是标准json，属于脏数据，将其放到侧输出流，放入的是jsonStr
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        // jsonObjDS.print("主流");
        // dirtyDS.print("脏数据");
        // 将侧输出流中脏数据写到Kafka对应主题
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        // 将主流返回
        return jsonObjDS;
    }
}
