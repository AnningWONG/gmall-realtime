package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.dwd.db.split.function.BaseDbTableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: DwdBaseDb
 * Package: com.atguigu.gmall.realtime.dwd.db.split.app
 * Description:
 *      事实表动态分流处理
 * @Author Wang Anning
 * @Create 2024/4/22 15:07
 * @Version 1.0
 */
public class DwdBaseDb extends BaseApp {

    public static void main(String[] args) {
        new DwdBaseDb().start(10019,
                4,
                "dwd_base_db",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. 对流中数据类型进行转换和ETL jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            // 类型转换
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            // ETL：不要历史数据
                            String type = jsonObj.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("不是标准json");
                        }
                    }
                }
        );
        jsonObjDS.print();
        // TODO 2. 使用Flink CDC从配置表中读取配置信息
        // 2.1 创建MySqlSource对象
        MySqlSource<String> mySqlSource
                = FlinkSourceUtil.getMySqlSource("gmall_config", "gmall_config.table_process_dwd");
        // 2.2 读取数据，封装为流
        DataStreamSource<String> mysqlStrDS
                = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        // mysqlStrDS.print();
        // 2.3 对当前流中数据类型进行转换 jsonStr -> 实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tableProcessDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {

                    @Override
                    public TableProcessDwd map(String jsonStr) throws Exception {
                        // 将字符串转换成json对象，方便获取属性
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 根据操作类型判断是从before还是after属性中获取配置表数据
                        String op = jsonObj.getString("op");
                        TableProcessDwd tableProcess = null;
                        if ("d".equals(op)) {
                            // 说明从配置表删除了1条信息，需要从before属性中获取配置表的数据
                            tableProcess = jsonObj.getObject("before", TableProcessDwd.class);
                        } else {
                            // 说明从配置表读取了信息或者向配置表中添加或修改了数据，需要从after属性中获取配置表的数据
                            tableProcess = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tableProcess.setOp(op);
                        return tableProcess;
                    }
                }
        );
        // tableProcessDS.print();
        // TODO 3. 广播配置表数据（broadcast算子）
        // 广播状态描述器
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDwd.class);
        // 广播
        BroadcastStream<TableProcessDwd> broadcastDS = tableProcessDS.broadcast(mapStateDescriptor);
        // TODO 4. 将主流业务数据和广播流配置信息进行关联（connect算子）
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectedDS = jsonObjDS.connect(broadcastDS);
        // TODO 5. 对关联的数据进行处理（process算子）
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultDS = connectedDS.process(
                new BaseDbTableProcessFunction(mapStateDescriptor)
        );

        // resultDS.print();
        // TODO 6. 将流中数据写到Kafka不同主题中
        resultDS.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
