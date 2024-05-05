package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdTradeOrderPaySucDetail
 * Package: com.atguigu.gmall.realtime.dwd.db.app
 * Description:
 *      支付成功事实表
 *      需要启动 ZooKeeper，Kafka，Maxwell，HDFS，HBase，应用DwdTradeOrderDetail
 * @Author Wang Anning
 * @Create 2024/4/22 11:29
 * @Version 1.0
 */
public class DwdTradeOrderPaySucDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 1. 读取下单事实表数据
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts, 0), " +
                        "watermark for et as et - interval '3' second " + // 水位线生成策略，有界乱序
                        ")" + SqlUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        // TODO 2. 从Kafka的topic_db主题中读取数据
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
        // TODO 3. 过滤出支付成功行为
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] user_id," +
                "data['order_id'] order_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "`pt`," +
                "ts, " +
                "et " +
                "from topic_db " +
                "where `table`='payment_info' " +
                "and `type`='update' " +
                "and `old`['payment_status'] is not null " +
                "and `data`['payment_status']='1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // TODO 4. 从HBase中读取字典表数据
        readBaseDic(tableEnv);
        // TODO 5. 关联（interval join）
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code ");

        // TODO 6. 写入Kafka
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint, " +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED" +
                ")" + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
}
