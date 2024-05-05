package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * ClassName: DimAsyncFunction
 * Package: com.atguigu.gmall.realtime.common.function
 * Description:
 *      发送异步请求进行维度关联的模板类
 * @Author Wang Anning
 * @Create 2024/4/28 14:24
 * @Version 1.0
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimFunction<T>{
    private AsyncConnection asyncHBaseConn;
    private StatefulRedisConnection<String, String> asyncRedisConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncHBaseConn = HBaseUtil.getAsyncHBaseConnection();
        asyncRedisConn = RedisUtil.getAsyncRedisConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeAsyncHBaseConnection(asyncHBaseConn);
        RedisUtil.closeAsyncRedisConnection(asyncRedisConn);
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        // 创建异步编排对象，执行线程任务，有返回结果，返回结果将作为下一个线程任务的入参
        CompletableFuture.supplyAsync(
                // 无入参，有返回值
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        // 从Redis中获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(asyncRedisConn, getTableName(), getRowKey(obj));
                        return dimJsonObj;
                    }
                }
        ).thenApplyAsync(
                // 有入参有返回值
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJsonObj) {
                        if (dimJsonObj != null) {
                            // Redis中查询到维度数据----缓存命中
                            System.out.println("从Redis中获取到了" + getTableName() + "表的主键为" + getRowKey(obj) + "的数据");
                        } else {
                            // Redis中未查询到维度数据，发送请求到HBase查询维度数据
                            dimJsonObj = HBaseUtil.getRowAsync(asyncHBaseConn, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(obj));
                            if (dimJsonObj != null) {
                                System.out.println("从HBase中获取到了" + getTableName() + "表的主键为" + getRowKey(obj) + "的数据");
                                // 查询到的维度数据放到Redis中缓存，方便下次使用
                                RedisUtil.writeDimAsync(asyncRedisConn,getTableName(),getRowKey(obj),dimJsonObj);
                            } else {
                                System.out.println("未查找到" + getTableName() + "表的主键为" + getRowKey(obj) + "的数据");
                            }
                        }

                        return dimJsonObj;
                    }
                }
        ).thenAcceptAsync(
                // 有入参，无返回值
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJsonObj) {
                        if (dimJsonObj != null) {
                            // 根据查询到的维度数据补充流中对象的维度属性
                            addDim(obj,dimJsonObj);
                        }
                        // 将关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        throw new RuntimeException("如果异步关联维度超时，可以做如下检查：\n" +
                "1. 查看需要的进程是否都启动，包括ZooKeeper，Kafka，HDFS，HBase，Redis\n" +
                "2. 查看Redis中bind的配置，是否注掉，或者为0.0.0.0\n" +
                "3. 查看HBase维度表中是否存在维度数据");
    }
}
