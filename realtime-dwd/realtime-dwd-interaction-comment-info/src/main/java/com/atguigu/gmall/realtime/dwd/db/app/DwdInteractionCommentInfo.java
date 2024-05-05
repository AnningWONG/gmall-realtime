package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdInteractionCommentInfo
 * Package: com.atguigu.gmall.realtime.dwd.db.app
 * Description:
 *      评论事实表
 *      需要启动ZooKeeper，Kafka，Maxwell，HDFS，HBase
 * @Author Wang Anning
 * @Create 2024/4/20 11:19
 * @Version 1.0
 */
public class DwdInteractionCommentInfo extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 3. 从Kafka topic_db主题读取数据，创建动态表 ---kafka连接器
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);


        // TODO 4. 过滤评论数据
        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "`data`['id'] id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['sku_id'] sku_id,\n" +
                "`data`['appraise'] appraise,\n" +
                "`data`['comment_txt'] comment_txt,\n" +
                "ts,\n" +
                "pt\n" +
                "from topic_db where `table` = 'comment_info' and `type` = 'insert'");
        // commentInfo.execute().print();
        tableEnv.createTemporaryView("comment_info",commentInfo);


        // TODO 5. 从HBase中查询字典数据，创建动态表 ---hbase连接器
        readBaseDic(tableEnv);


        // TODO 6. 将评论和字典表数据进行关联，---LookUp Join
        Table joinedTable = tableEnv.sqlQuery("SELECT \n" +
                "\tid,\n" +
                "\tuser_id,\n" +
                "\tsku_id,\n" +
                "\tappraise,\n" +
                "\td.dic_name appraise_name,\n" +
                "\tcomment_txt,\n" +
                "\tts\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS d\n" +
                "    ON c.appraise = d.dic_code");
        // joinedTable.execute().print();


        // TODO 7. 将关联结果写到Kafka主题中 ---upsert-kafka连接器
        // 7.1 创建动态表和写入的主题映射
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                "\tid string,\n" +
                "\tuser_id string,\n" +
                "\tsku_id string,\n" +
                "\tappraise string,\n" +
                "\tappraise_name string,\n" +
                "\tcomment_txt string,\n" +
                "\tts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 7.2 执行写入Kafka操作
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }




}
