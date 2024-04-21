package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdTradeCartAdd
 * Package: com.atguigu.gmall.realtime.dwd.db.app
 * Description:
 *      加购事实表
 * @Author Wang Anning
 * @Create 2024/4/20 15:43
 * @Version 1.0
 */
public class DwdTradeCartAdd extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 1. 从Kafka读取数据创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);
        // TODO 2. 过滤加购行为
        // table='cart_info'  type='insert' or 'update'(old['sku_num'] is not null and data['sku_num'] > old['sku_num'])
        Table cartInfo = tableEnv.sqlQuery("select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "\tif(`type`='insert',`data`['sku_num'],cast((cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num,\n" +
                "\tts\n" +
                "from topic_db where `table`='cart_info'\n" +
                "and (`type`='insert' or (`type`='update' and `old`['sku_num'] is not null \n" +
                "and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        // cartInfo.execute().print();
        // TODO 3. 将过滤出的加购数据写到Kafka主题
        // 创建动态表和写入的数据映射
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_CART_ADD + " (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  sku_num STRING,\n" +
                "  ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        // 写入Kafka
        tableEnv.createTemporaryView("cart_info",cartInfo);
        tableEnv.executeSql("insert into " + Constant.TOPIC_DWD_TRADE_CART_ADD + " select * from cart_info");
    }
}
