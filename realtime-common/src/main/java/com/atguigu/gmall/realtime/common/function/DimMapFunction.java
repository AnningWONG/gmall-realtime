package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * ClassName: DimMapFunction
 * Package: com.atguigu.gmall.realtime.common.function
 * Description:
 *      旁路缓存关联维度的模板
 * @Author Wang Anning
 * @Create 2024/4/28 9:36
 * @Version 1.0
 */
public abstract class DimMapFunction<T> extends RichMapFunction<T,T> implements DimFunction<T>{
    private Connection hbaseConn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
        RedisUtil.closeJedis(jedis);
    }
    @Override
    public T map(T obj) throws Exception {
        // 根据流中对象获取要关联的维度主键
        String key = getRowKey(obj);
        // 从Redis中获取要关联的维度
        JSONObject dimJsonObj = RedisUtil.readDim(jedis, getTableName(), key);
        if (dimJsonObj != null) {
            // 从Redis中获取到了维度数据----缓存命中
            System.out.println("从Redis中获取到了" + getTableName() + "表的主键为" + key + "的数据");
        } else {
            // Redis缓存中没有获取到维度数据，发送请求到HBase中获取维度数据
            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, getTableName(), key, JSONObject.class, false);
            if (dimJsonObj != null) {
                System.out.println("从HBase中获取到了" + getTableName() + "表的主键为" + key + "的数据");
                // 将从HBase中查询出来的数据放到Redis中缓存起来，方便下次使用
                RedisUtil.writeDim(jedis,getTableName(),key,dimJsonObj);
            } else {
                System.out.println("未获取到" + getTableName() + "表的主键为" + key + "的数据");
            }
        }
        // 将维度属性补充到流中对象上
        if (dimJsonObj != null) {
            addDim(obj,dimJsonObj);
        }
        return obj;
    }


}
