package com.atguigu.gmall.realtime.dim.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.dim.function.HBaseSinkFunction;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * ClassName: DimApp
 * Package: com.atguigu.gmall.realtime.dim.app
 * Description:
 *      维度层处理：执行本程序前需要启动ZooKeeper，Kafka，Maxwell，HDFS，HBase
 *      MySQL开机自启
 *      启动Maxwell前应该先启动Kafka
 *      启动Kafka前必须先启动ZooKeeper
 *      启动HBase前必须先启动ZooKeeper和HDFS
 *
 *      首次执行要先在HBase创建命名空间gmall
 * @Author Wang Anning
 * @Create 2024/4/16 11:41
 * @Version 1.0
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001,4,"dim_app",Constant.TOPIC_DB);
    }

    /**
     * 如何测试代码核心逻辑：
     *      逐步测试，每步测试只打开必要的代码，输入少量的数据
     * @param env
     * @param kafkaStrDS
     */
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 4. 对读取的数据进行类型转换 jsonStr -> jsonObj，并进行简单的ETL清洗（脏数据可存可删，本作业中选择去掉脏数据）
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);
        // 测试上面这行代码需要把这一行打开，并将etl方法中最后面print那一行也打开，测试完将print那一行重新注释掉
        // 对一张表执行Maxwell-bootstrap，或者生成一些数据到业务数据库中，或者只是修改业务数据库中的一个值，查看输出即可
        // 执行程序后输出：
        /*
        4> {"database":"gmall","xid":2358,
            "data":{"tm_name":"Redmi",
                    "create_time":"2021-12-14 00:00:00",
                    "logo_url":"用于测试代码的修改",
                    "id":1},
            "old":{},"commit":true,"type":"update","table":"base_trademark","ts":1713355591}
        */



        // TODO 5. 使用Flink CDC读取配置表中的配置信息
        // TODO 6. 对读取的配置流数据进行类型转换 jsonStr -> 实体类对象
        SingleOutputStreamOperator<TableProcessDim> tableProcessDS = readTableProcess(env);
        // 测试上面这行代码需要把这一行打开，并将readTableProcess方法中最后面print那一行也打开，测试完将print那一行重新注释掉
        // 执行程序后输出：
        /*
        先全表扫描，op=r
        3> TableProcessDim(sourceTable=base_category1, sinkTable=dim_base_category1,
                           sinkColumns=id,name, sinkFamily=info, sinkRowKey=id, op=r)
        ...
        Apr 18, 2024 9:39:32 AM com.github.shyiko.mysql.binlog.BinaryLogClient connect
        INFO: Connected to hadoop102:3306 at mysql-bin.000045/157 (sid:5825, cid:30)
        对配置表进行C | U | D后分别输出：
        3> TableProcessDim(sourceTable=测试, sinkTable=测试, sinkColumns=测试, sinkFamily=测试, sinkRowKey=测试, op=c)
        4> TableProcessDim(sourceTable=测试, sinkTable=测试, sinkColumns=测试, sinkFamily=test, sinkRowKey=测试, op=u)
        1> TableProcessDim(sourceTable=测试, sinkTable=测试, sinkColumns=测试, sinkFamily=test, sinkRowKey=测试, op=d)
        */



        // TODO 7. 根据配置流中的数据到HBase中执行建表或删除操作
        tableProcessDS = createHBaseTable(tableProcessDS);
        // 测试上面这行代码除了需要将这行代码的注释打开，还要将56步对应的代码打开，
        // 并将createHBaseTable方法中最后面print那一行也打开，测试完将print那一行重新注释掉
        // 执行程序后输出：
        /*
        因为已经创建过表，所以先输出表已存在
        Apr 18, 2024 9:51:24 AM com.github.shyiko.mysql.binlog.BinaryLogClient connect
        INFO: Connected to hadoop102:3306 at mysql-bin.000045/1266 (sid:5998, cid:43)
        要创建的gmall下的表dim_activity_info已经存在
        3> TableProcessDim(sourceTable=activity_info, sinkTable=dim_activity_info,
                           sinkColumns=id,activity_name,activity_type,activity_desc,
                           start_time,end_time,create_time, sinkFamily=info, sinkRowKey=id, op=r)
        ...
        对配置表进行C | U | D后分别输出：
        创建gmall下的表test
        2> TableProcessDim(sourceTable=test, sinkTable=test, sinkColumns=test, sinkFamily=test, sinkRowKey=test, op=c)
        删除gmall下的表test
        创建gmall下的表test
        3> TableProcessDim(sourceTable=测试, sinkTable=test, sinkColumns=test, sinkFamily=test, sinkRowKey=test, op=u)
        删除gmall下的表test
        4> TableProcessDim(sourceTable=测试, sinkTable=test, sinkColumns=test, sinkFamily=test, sinkRowKey=test, op=d)
        */




        // TODO 8. 将配置流进行广播
        // TODO 9. 将主流业务数据与广播流配置信息进行关联
        // TODO 10. 对关联后的数据进行处理，直接调process方法，传入BroadcastProcessFunction
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processDS = connect(jsonObjDS, tableProcessDS);
        // 测试上面这行代码除了需要将这行及以上所有代码的注释打开
        // 并将connect方法中最后面print那一行也打开，测试完将print那一行重新注释掉
        // 执行程序后输出：
        /*
        因为主流没有新数据，所以先输出：
        Apr 18, 2024 10:10:44 AM com.github.shyiko.mysql.binlog.BinaryLogClient connect
        INFO: Connected to hadoop102:3306 at mysql-bin.000045/2343 (sid:6001, cid:53)
        要创建的gmall下的表dim_base_category1已经存在
        ...
        对业务表进行修改后，输出：
        4> ({"tm_name":"Redmi","id":1,"type":"update"},
            TableProcessDim(sourceTable=base_trademark, sinkTable=dim_base_trademark,
                            sinkColumns=id,tm_name, sinkFamily=info, sinkRowKey=id, op=r))
        */




        // TODO 11. 将维度数据同步到HBase表中
        // Flink DataStream API没有HBase连接器
        processDS.addSink(
                // 将流中数据同步到HBase表中
                new HBaseSinkFunction()
        );
        // 测试上面这行代码除了需要将这行及以上所有代码的注释打开
        // 可以清空模拟的业务和日志数据，重新模拟，然后用Maxwell-bootstrap同步，查看HBase表中是否有数据
    }
    // TODO 8. 将配置流进行广播
    // TODO 9. 将主流业务数据与广播流配置信息进行关联
    // TODO 10. 对关联后的数据进行处理，直接调process方法，传入BroadcastProcessFunction
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tableProcessDS) {
        // TODO 8. 将配置流进行广播
        // 有广播状态的广播需要创建一个状态描述器
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tableProcessDS.broadcast(mapStateDescriptor);
        // TODO 9. 将主流业务数据与广播流配置信息进行关联
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
        // TODO 10. 对关联后的数据进行处理，直接调process方法，传入BroadcastProcessFunction
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processDS = connectDS.process(
                // 将逻辑封装到函数中，参数是一个状态描述器对象
                new TableProcessFunction(mapStateDescriptor)
        );
        // processDS.print();
        return processDS;
    }
    // TODO 7. 根据配置流中的数据到HBase中执行建表或删除操作
    private SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tableProcessDS) {
        // 获取和关闭HBase连接、建表删表操作都封装到HBaseUtil工具类中，放在common module下
        tableProcessDS = tableProcessDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    // 避免每来一条数据就创建HBase连接，让创建连接的动作只在open方法执行一次
                    Connection hBaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        // 获取对配置表数据的操作类型
                        String op = tableProcessDim.getOp();
                        // 获取在HBase中建表的表名
                        String sinkTable = tableProcessDim.getSinkTable();
                        // 获取在HBase中建表的列族
                        String[] families = tableProcessDim.getSinkFamily().split(",");
                        if ("r".equals(op) || "c".equals(op)) {
                            // c和r
                            // 在HBase中创建表
                            HBaseUtil.createHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,families);
                        } else if ("d".equals(op)){
                            // d
                            // 从HBase中删除表
                            HBaseUtil.dropHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        } else {
                            // u，更新操作
                            // 先将HBase表删除
                            HBaseUtil.dropHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            // 再创建表
                            HBaseUtil.createHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,families);
                        }
                        return tableProcessDim;
                    }
                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hBaseConn);
                    }
                }
        );
        // print测试用，如果map算子执行完毕，就输出TableProcessDim对象到控制台
        // tableProcessDS.print();
        return tableProcessDS;
    }
    // TODO 5. 使用Flink CDC读取配置表中的配置信息
    // TODO 6. 对读取的配置流数据进行类型转换 jsonStr -> 实体类对象
    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        // TODO 5. 使用Flink CDC读取配置表中的配置信息
        // 5.1 创建MySqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil
                .getMySqlSource("gmall_config", "gmall_config.table_process_dim");
        // 5.2 读取数据封装为流，并行度为1，将配置流转换为广播流时并行度也会自动变为1，这里直接创建为1
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
        // TODO 6. 对读取的配置流数据进行类型转换 jsonStr -> 实体类对象
        // 不同操作（crud）的转化逻辑略有不同
        SingleOutputStreamOperator<TableProcessDim> tableProcessDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        // 如果是RCU操作，即读和插入和新增，属性从after中获取；如果是d，即删除，属性从before中获取
                        // 为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        // 获取操作类型（CRUD）
                        String op = jsonObject.getString("op");

                        TableProcessDim tableProcessDim;
                        if ("d".equals(op)) {
                            // 说明从配置表中删除一条记录，应该从当前json的before属性中获取删除前的配置内容
                            // 注意：fastjson底层自动帮我们做了下划线到驼峰命名法的转化
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            // 说明从配置表中进行了 C | R | U的操作，应该从当前json的after属性中获取配置内容
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        // print测试用，输出TableProcessDim对象到控制台
        // tableProcessDS.print();
        return tableProcessDS;
    }
    // TODO 4. 对读取的数据进行类型转换 jsonStr -> jsonObj，并进行简单的ETL清洗（脏数据可存可删，本作业中选择去掉脏数据）
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                        try {
                            // jsonStr -> jsonObj
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            // 获取业务表中被改变的行所在的数据库
                            String db = jsonObject.getString("database");
                            // 获取操作类型
                            String type = jsonObject.getString("type");
                            // 获取行数据
                            String data = jsonObject.getString("data");
                            // ETL清洗
                            // 只要来自于gmall的数据，不要来自于其他库，如gmall_config的数据
                            // 去掉操作类型字段值是bootstrap-start，bootstrap-complete的数据
                            // 过滤空值数据
                            if ("gmall".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2
                            ) {
                                out.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            // 当Json解析出现错误时
                            throw new RuntimeException("不是标准json");
                        }
                    }
                }
        );
        // print测试用，输出json对象到控制台
        // jsonObjDS.print();
        return jsonObjDS;
    }
}
