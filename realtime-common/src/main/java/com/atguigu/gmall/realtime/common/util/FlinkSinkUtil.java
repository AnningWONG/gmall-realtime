package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

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
}
