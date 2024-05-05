package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSqlApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import com.atguigu.gmall.realtime.dws.function.KeywordUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwsTrafficSourceKeywordPageViewWindow
 * Package: com.atguigu.gmall.realtime.dws.app
 * Description:
 *      搜索关键词聚合统计
 * @Author Wang Anning
 * @Create 2024/4/24 10:50
 * @Version 1.0
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSqlApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 1. 注册自定义函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);



        // TODO 2. 从DWD的页面日志主题中读取数据，创建动态表，并指定事件时间和水位线生成策略
        tableEnv.executeSql("CREATE TABLE page_log (\n" +
                "  `common` map<string,string>,\n" +
                "  `page` map<string,string>,\n" +
                "  `ts` bigint,\n" +
                "  et as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "  WATERMARK FOR et AS et\n" +
                ")" + SqlUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE, "dws_traffic_source_keyword_page_view_window"));
        // tableEnv.executeSql("select * from page_log").print();



        // TODO 3. 过滤搜索行为
        // 上一页是查询last_page_id = 'search', 搜索类型item_type = 'keyword', 搜索内容item is not null
        // executeSql可以执行多种语句，返回结果集；sqlQuery只能查询，返回动态表对象
        Table searchTable = tableEnv.sqlQuery("select\n" +
                "page['item'] fullword,\n" +
                "et\n" +
                "from page_log where page['last_page_id']='search' \n" +
                "and page['item_type']='keyword' and page['item'] is not null");
        // searchTable.execute().print();
        tableEnv.createTemporaryView("search_table",searchTable);



        // TODO 4. 对搜索内容进行分词，和原表字段关联
        Table splitTable = tableEnv.sqlQuery("SELECT\n" +
                "t.keyword,\n" +
                "search_table.et\n" +
                "FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)"); // t是临时表表名，keyword是临时表中字段名
        tableEnv.createTemporaryView("split_table", splitTable);
        // tableEnv.executeSql("select * from split_table").print();



        // TODO 5. 分组开窗聚合
        // 10s 滚动事件时间窗口
        // Flink SQL中的日期时间类型TIMESTAMP和Doris中DATETIME的可能无法转换，所以需要用DATE_FORMAT函数提前转化
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') stt, \n" +
                "DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') edt, \n" +
                "DATE_FORMAT(window_start, 'yyyy-MM-dd') cur_date, \n" +
                "keyword,\n" +
                "count(*) keyword_count\n" +
                "FROM TABLE(\n" +
                "TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "GROUP BY window_start, window_end, keyword");
        tableEnv.createTemporaryView("res_table",resTable);
        // tableEnv.executeSql("select * from res_table").print();



        // TODO 6. 将聚合的结果写入Doris
        tableEnv.executeSql("CREATE TABLE dws_traffic_source_keyword_page_view_window (  " +
                "    stt string,  " +
                "    edt string,  " +
                "    cur_date string,  " +
                "    keyword string,  " +
                "    keyword_count bigint  " +
                ")" + SqlUtil.getDorisSinkDDL("dws_traffic_source_keyword_page_view_window"));
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}
