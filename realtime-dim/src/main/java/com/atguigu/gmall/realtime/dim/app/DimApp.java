package com.atguigu.gmall.realtime.dim.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.google.common.base.CaseFormat;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Properties;

/**
 * ClassName: DimApp
 * Package: com.atguigu.gmall.realtime.dim.app
 * Description:
 * 维度层处理：执行本程序前需要启动ZooKeeper，Kafka，Maxwell，HDFS，HBase
 *
 * @Author Wang Anning
 * @Create 2024/4/16 11:41
 * @Version 1.0
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境准备
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // TODO 2. 检查点相关设置
        // 2.1 开启检查点，指定检查点时间间隔，默认模式就是barrier对齐的精准一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2.2 设置检查点超时时间，应该长一点，因为默认检查点失败1次整个Job就会失败，此处设置为1min
        checkpointConfig.setCheckpointTimeout(60000L);
        // 2.3 设置job取消后，检查点是否保留，保留后就可以从检查点恢复Job
        checkpointConfig.
                setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置两个检查点之间最小时间间隔，这是前1个检查点的结束时间和后1个检查点的开始时间最小间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // 2.5 设置Job的重启策略
        // 每30天只允许失败3次，重试间隔3s
        // env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3000L)));
        // 3次重试机会，间隔3s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        // 2.6 设置状态后端，指定检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");
        // 2.7 设置操作Hadoop用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        // TODO 3. 从Kafka主题中读取数据
        // 3.1 声明消费主题和消费者组
        String groupId = "dim_app_group";
        // 3.2 创建消费者对象KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId(groupId)
                // 先从消费者提交的位点消费，如果没提交过位点，就从最早的偏移量开始消费
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                // 注意：SimpleStringSchema的deserialize方法不允许传入的字节数组为空，从Kafka中读取到空消息时会出现问题
                // .setValueOnlyDeserializer(new SimpleStringSchema())
                // 所以自定义一个反序列化器
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message);
                        }
                        // Flink会把空字符数组自动丢弃，而不会抛异常
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        // 3.3 消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        // TODO 4. 对读取的数据进行类型转换 jsonStr -> jsonObj，并进行简单的ETL清洗（脏数据可存可删，本作业中选择去掉脏数据）
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
                            // 不要操作类型字段值是bootstrap-start，bootstrap-complete的数据
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
                            throw new RuntimeException("不是标准json");
                        }
                    }
                }
        );
        // print测试用，输出json对象到控制台
        // jsonObjDS.print();

        // TODO 5. 使用Flink CDC读取配置表中的配置信息
        // 5.1 创建MySqlSource对象
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 起点的配置默认值就是initial
                // .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
        // 5.2 读取数据封装为流，并行度为1，因为后续还要广播里面的数据
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
        // TODO 7. 根据配置流中的数据到HBase中执行建表或删除操作
        // 获取关闭HBase连接、建表删表操作都封装到HBaseUtil工具类中，放在common module下
        tableProcessDS = tableProcessDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    Connection hBaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        // 获取配置表数据的操作类型
                        String op = tableProcessDim.getOp();

                        // 获取在HBase中建表的表名
                        String sinkTable = tableProcessDim.getSinkTable();
                        // 获取在HBase中建表的列族
                        String[] families = tableProcessDim.getSinkFamily().split(",");
                        if ("r".equals(op) || "c".equals(op)) {
                            // 在HBase中创建表
                            HBaseUtil.createHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,families);
                        } else if ("d".equals(op)){
                            // 从HBase中删除表
                            HBaseUtil.dropHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        } else {
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
        tableProcessDS.print();
        // TODO 8. 将配置流进行广播
        // TODO 9. 将主流业务数据与广播流配置信息进行关联
        // TODO 10. 对关联后的数据进行处理，直接调process方法，传入BroadcastProcessFunction
        // 实现processElement方法：处理主流业务数据
        // 根据处理的表名，到广播状态中获取对应的配置信息，获取到配置，说明是维度数据，将数据传递到下游
        // 实现processBroadElement方法
        // 将流中配置信息放到广播状态中
        // TODO 11. 将维度数据同步到HBase表中

        env.execute();
    }
}
