package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: DwdTradeOrderDetail
 * Package: com.atguigu.gmall.realtime.dwd.db.app
 * Description:
 *      下单事实表
 *      启动程序前需要启动 ZooKeeper，Kafka，Maxwell
 * @Author Wang Anning
 * @Create 2024/4/22 9:42
 * @Version 1.0
 */
public class DwdTradeOrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014, 4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 0. 普通Join，避免内存占满，需要设置状态失效时间
        // 失效时间需要考虑传输的延迟 和 业务上的滞后关系，本需求只需要考虑传输延迟
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // TODO 1. 从topic_db主题读取数据创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        // TODO 2. 过滤
        // 过滤出 order_detail表， 操作类型是 insert
        Table orderDetail = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['sku_name'] sku_name," +
                        "data['create_time'] create_time," +
                        "data['source_id'] source_id," +
                        "data['source_type'] source_type," +
                        "data['sku_num'] sku_num," +
                        "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                        "   cast(data['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "data['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "data['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "data['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts " +
                        "from topic_db " +
                        "where `table`='order_detail' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        // 过滤出 order_info表， 操作类型是 insert
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id " +
                        "from topic_db " +
                        "where `table`='order_info' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // 过滤出order_detail_activity表，操作类型是insert
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['activity_id'] activity_id, " +
                        "data['activity_rule_id'] activity_rule_id " +
                        "from topic_db " +
                        "where `table`='order_detail_activity' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 过滤出order_detail_coupon表，操作类型是insert
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['coupon_id'] coupon_id " +
                        "from topic_db " +
                        "where `table`='order_detail_coupon' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
        // orderDetailCoupon.execute().print();

        // TODO 3. 关联4张表（left join）
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");

        // TODO 4. 关联结果写入Kafka主题中，主题用事务表名命名
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
                        "primary key(id) not enforced " +  // 用upsert-kafka必须指定Sink表的主键
                        ")" + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

    }
}
