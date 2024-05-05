package com.atguigu.gmall.realtime.dwd.db.split.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName: BaseDbTableProcessFunction
 * Package: com.atguigu.gmall.realtime.dwd.db.split.function
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/4/22 16:08
 * @Version 1.0
 */
public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {
    // 广播状态描述器
    MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;
    // 存放预加载数据的普通变量
    private Map<String,TableProcessDwd> configMap = new HashMap<>();
    // 广播状态描述器需要在构造函数对象的时候传进来
    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    // 预加载配置表数据，避免主流数据因为过早抵达，关联不上而丢失
    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取连接
        Connection mysqlConn = JdbcUtil.getMysqlConnection();
        // 查询
        List<TableProcessDwd> tpList = JdbcUtil.queryList(mysqlConn,
                "select * from gmall_config.table_process_dwd", TableProcessDwd.class,true);
        // 查询结果放到普通变量configMap中
        for (TableProcessDwd tableProcessDwd : tpList) {
            String sourceTable = tableProcessDwd.getSourceTable();
            String sourceType = tableProcessDwd.getSourceType();
            String key = getKey(sourceTable, sourceType);
            configMap.put(key, tableProcessDwd);
        }
        // 关闭连接
        JdbcUtil.closeConnection(mysqlConn);
    }
    // 对主流的处理
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        // 获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 根据表名字段和操作类型获取key
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        String key = getKey(table, type);
        // 根据key到广播状态及configMap中获取对应的配置信息，只有获取到了，才将数据向下游传递
        TableProcessDwd tableProcessDwd = null;
        if ((tableProcessDwd = broadcastState.get(key)) != null || (tableProcessDwd = configMap.get(key)) != null){
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            String sinkCols = tableProcessDwd.getSinkColumns();
            // 删除不需要的列
            deleteNotNeedCol(dataJsonObj,sinkCols);
            // 补充ts字段
            Long ts = jsonObj.getLong("ts");
            dataJsonObj.put("ts", ts);
            // 向下游传递数据
            out.collect(Tuple2.of(dataJsonObj,tableProcessDwd));
        }
    }
    // 删除不需要的列的方法
    private void deleteNotNeedCol(JSONObject dataJsonObj, String sinkCols) {
        List<String> cols = Arrays.asList(sinkCols.split(","));
        dataJsonObj.entrySet().removeIf(entry -> !cols.contains(entry.getKey()));
    }
    // 对广播流的处理
    @Override
    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        // 获取广播状态
        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 根据来源表名和来源类型生成状态的Key
        String key = getKey(tp.getSourceTable(), tp.getSourceType());
        // 同步配置表的操作，到状态和configMap中
        if ("d".equals(tp.getOp())) {
            broadcastState.remove(key);
            configMap.remove(key);
        } else {
            broadcastState.put(key, tp);
            configMap.put(key,tp);
        }
    }

    // 生成状态和configMap中Key的方法
    private String getKey(String sourceTable, String sourceType) {
        return sourceTable + ":" + sourceType;
    }
}
