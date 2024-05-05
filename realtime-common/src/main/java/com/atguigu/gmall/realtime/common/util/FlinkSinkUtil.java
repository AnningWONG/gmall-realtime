package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * ClassName: FlinkSinkUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *      获取Flink的Sink的工具类
 * @Author Wang Anning
 * @Create 2024/4/19 10:40
 * @Version 1.0
 */
public class FlinkSinkUtil {
    // 获取KafkaSink
    public static KafkaSink<String> getKafkaSink (String topic) {
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 精准一次
                // 1. 开启检查点
                // 2. 开启事务
                // .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 3. 事务前缀
                // .setTransactionalIdPrefix("xxxx")
                // 4. 事务超时时间
                // .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15 * 60 * 1000 + "")
                // 5. 消费者消费策略读已提交
                // 此处不开启事务，因为测试代码需要频繁启停程序，如果开启事务每次启动程序，程序都想要从检查点恢复未提交成功的事务
                // 如果这个程序不是从检查点恢复的，就无法恢复事务，程序卡死
                // 如果启动程序时存在未提交成功的事务，又恢复不了怎么办，删掉Kafka用于保存未提交成功的事务的主题
                .build();
        return kafkaSink;
    }

    // 获取KafkaSink，将数据根据内容发送到不同的Kafka主题中
    public static KafkaSink<Tuple2<JSONObject,TableProcessDwd>> getKafkaSink() {
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        // 自定义序列化模式
                        new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tup2, KafkaSinkContext context, Long timestamp) {
                                // 获取数据
                                JSONObject dataJsonObj = tup2.f0;
                                // 获取主题，用事实表的表名做主题，表名被记录在配置对象里
                                TableProcessDwd tp = tup2.f1;
                                String topic = tp.getSinkTable();
                                // 生成一条记录并返回
                                return new ProducerRecord<>(topic, dataJsonObj.toJSONString().getBytes());
                            }
                        }
                )
                .build();
        return kafkaSink;
    }

    // 获取KafkaSink，将数据根据内容发送到不同的Kafka主题中，如果流中数据类型不确定，用这个方法
    // 根据流中数据类型，创建对应的序列化模式，作为调用这个KafkaSink方法的参数
    public static <T> KafkaSink<T> getKafkaSink (KafkaRecordSerializationSchema krs) {
        KafkaSink<T> kafkaSink = KafkaSink.<T>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(krs)
                .build();
        return kafkaSink;
    }
    // 获取DorisSink
    public static DorisSink<String> getDorisSink (String tableName) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                // .setDorisReadOptions(DorisReadOptions.builder().build()) 可以不写
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(Constant.DORIS_FE_NODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE + "." + tableName)
                        .setUsername("root")
                        .setPassword("aaaaaa")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(8*1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔   三个对批次的限制是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return sink;
    }

}
