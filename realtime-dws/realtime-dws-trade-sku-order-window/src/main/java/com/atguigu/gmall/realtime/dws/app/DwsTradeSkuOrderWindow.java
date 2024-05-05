package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunc;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.function.DimMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsTradeSkuOrderWindow
 * Package: com.atguigu.gmall.realtime.dws.app
 * Description:
 *      sku粒度下单业务过程聚合统计
 * @Author Wang Anning
 * @Create 2024/4/27 10:07
 * @Version 1.0
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. 处理空消息，并对流中数据进行类型转换 jsonStr -> jsonObj
        // 因为Table API才可以自动忽略空消息，而DataStream API不能自动忽略空消息
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
        // jsonObjDS.print();
        // TODO 2. 去重
        // 2.1 按照唯一键（订单明细ID）分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        // 2.2 去重方案1：状态 + 定时器
        /*
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 从状态中获取这个订单明细ID的上条数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            // 状态中没有数据，说明当前数据是第1条，将当前数据放到状态中
                            lastJsonObjState.update(jsonObj);
                            // 并注册5s后执行的定时器，时间到则将状态中的数据向下游传递
                            TimerService timerService = ctx.timerService();
                            long currentProcessingTime = timerService.currentProcessingTime();
                            timerService.registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            // 状态中有数据，说明重复，判断当前数据和状态中的数据哪一个聚合时间更大，将时间大的放到状态中
                            // 伪代码，事实表中没有添加聚合时间戳字段：
                            String curTs = jsonObj.getString("聚合时间戳");
                            String lastTs = lastJsonObj.getString("聚合时间戳");
                            if (curTs.compareTo(lastTs) > 0) {
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    // 定时器触发后执行的方法
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 从状态中获取数据
                        JSONObject jsonObj = lastJsonObjState.value();
                        // 向下游传递
                        out.collect(jsonObj);
                        // 清空状态
                        lastJsonObjState.clear();
                    }
                }
        );
        */
        // 2.3 去重方案2：状态 + 抵消
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
                            String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                            String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        // 所有的数据都更新状态，并向下游传递
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        // distinctDS.print();
        // TODO 3. 指定Watermark生成策略，提取事件时间字段
        // 水位线生成策略：单调递增，事件时间字段是ts（秒为单位），转换为毫秒为单位
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
        // TODO 4. 对流中数据进行类型转换 jsonObj -> 实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = wmDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                        String skuId = jsonObj.getString("sku_id");
                        String splitOriginalAmount = jsonObj.getString("split_original_amount");
                        String splitCouponAmount = jsonObj.getString("split_coupon_amount");
                        String splitActivityAmount = jsonObj.getString("split_activity_amount");
                        String splitTotalAmount = jsonObj.getString("split_total_amount");
                        Long ts = jsonObj.getLong("ts") * 1000;
                        // TradeSkuOrderBean有@Builder注解
                        return TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(new BigDecimal(splitOriginalAmount))
                                .couponReduceAmount(new BigDecimal(splitCouponAmount))
                                .activityReduceAmount(new BigDecimal(splitActivityAmount))
                                .orderAmount(new BigDecimal(splitTotalAmount))
                                .ts(ts)
                                .build();
                    }
                }
        );
        // beanDS.print();
        // TODO 5. 按照统计维度（sku）分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);
        // TODO 6. 开窗
        // 10s滚动事件时间窗口
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowedDS =
                skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        // TODO 7. 聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reducedDS = windowedDS.reduce(
                // 聚合逻辑
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                // 补充字段值
                new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean orderBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );
        // reducedDS.print();
        // TODO 8. 关联sku维度
        // 维度关联最基本实现
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reducedDS.map(
                // 富函数，有open和close方法，用于创建和关闭HBase连接
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        // 获取关联字段
                        String skuId = orderBean.getSkuId();
                        // 关联字段即行键，获取HBase表中对应维度对象
                        JSONObject dimJsonObj = HBaseUtil.getRow(
                                hbaseConn,
                                Constant.HBASE_NAMESPACE,
                                "dim_sku_info",
                                skuId, JSONObject.class,
                                false
                        );
                        // 用关联到的维度对象中的属性补全主流中对象的属性
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));

                        return orderBean;
                    }
                }
        );
        */
        // withSkuInfoDS.print();

        // 优化1：加旁路缓存
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reducedDS.map(
                // 富函数，可以在open和close方法中创建和关闭HBase和Redis连接
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        // 根据流中对象获取要关联的维度主键
                        String skuId = orderBean.getSkuId();
                        // 先从Redis中获取要关联的维度
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            // 如果从Redis中获取到了维度数据，就是缓存命中
                            System.out.println("~~~~~~~从Redis中获取维度数据~~~~~~~");
                        } else {
                            // 如果从Redis中没有获取维度数据，发送请求从HBase中查询维度
                            dimJsonObj = HBaseUtil.getRow(
                                    hbaseConn,
                                    Constant.HBASE_NAMESPACE,
                                    "dim_sku_info",
                                    skuId,
                                    JSONObject.class);
                            if (dimJsonObj != null) {
                                // 如果从HBase中获取到了维度数据
                                System.out.println("~~~~~~~从HBase中获取维度数据~~~~~~~");
                                // 将维度数据放到Redis中缓存
                                RedisUtil.writeDim(jedis, "dim_sku_info", skuId, dimJsonObj);
                            } else {
                                // 如果从HBase和Redis都查询不到维度数据
                                System.out.println("~~~~~~未查询到要关联的维度数据~~~~~~");
                            }
                        }
                        // 根据维度对象属性补充流中对象的相关属性
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        return orderBean;
                    }
                }
        );
        */


        // 旁路缓存 + 模板方法
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reducedDS.map(
                new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDim(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                }
        );
        */

        // 优化2：异步IO
        // 将异步IO操作应用于DataStream，作为DataStream的一次转换操作
        // 这段代码只不过是调用了Redis和HBase工具类的异步方法
        // 没有在asyncInvoke方法里使用CompletableFuture异步编程，所以实际上还是同步的
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reducedDS,
                // 实现发送异步请求的AsyncFunction
                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private AsyncConnection asyncHBaseConn;
                    private StatefulRedisConnection<String, String> asyncRedisConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        asyncHBaseConn = HBaseUtil.getAsyncHBaseConnection();
                        asyncRedisConn = RedisUtil.getAsyncRedisConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeAsyncHBaseConnection(asyncHBaseConn);
                        RedisUtil.closeAsyncRedisConnection(asyncRedisConn);
                    }

                    @Override
                    public void asyncInvoke(TradeSkuOrderBean orderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                        // 根据流中对象获取要关联的维度主键
                        String skuId = orderBean.getSkuId();
                        // 从Redis中获取维度
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(asyncRedisConn, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            // Redis中查询到维度数据----缓存命中
                            System.out.println("~~~~~~~从Redis中获取维度数据~~~~~~~");
                        } else {
                            // Redis中未查询到维度数据，发送请求到HBase查询维度数据
                            dimJsonObj = HBaseUtil.getRowAsync(asyncHBaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId);
                            if (dimJsonObj != null) {
                                System.out.println("~~~~~~~从HBase中获取维度数据~~~~~~~");
                                // 查询到的维度数据放到Redis中缓存，方便下次使用
                                RedisUtil.writeDimAsync(asyncRedisConn, "dim_sku_info", skuId, dimJsonObj);
                            } else {
                                System.out.println("~~~~~要查找的维度数据不存在~~~~~");
                            }
                        }
                        // 根据查询到的维度数据补充流中对象的维度属性
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        // 将关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(orderBean));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        */


        // 异步IO + 模板方法
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reducedDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDim(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS

        );
        // withSkuInfoDS.print();


        // TODO 9. 关联spu维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDim(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setSpuName(dimJsonObj.getString("spu_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getSpuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 10. 关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean bean,
                                        JSONObject dim) {
                        bean.setTrademarkName(dim.getString("tm_name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // TODO 11. 关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory3DS = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // TODO 12. 关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory2DS = AsyncDataStream.unorderedWait(
                withCategory3DS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // TODO 13. 关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory1DS = AsyncDataStream.unorderedWait(
                withCategory2DS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        withCategory1DS.print();

        // TODO 14. 关联结果写入Doris
        withCategory1DS.map(new BeanToJsonStrMapFunc<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));

    }
}
