package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


/**
 * ClassName: RedisUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *      操作Redis的工具类
 * @Author Wang Anning
 * @Create 2024/4/27 14:33
 * @Version 1.0
 */
public class RedisUtil {
    public static JedisPool jedisPool;
    // 类加载时初始化JedisPool，使JedisPool只加载1次
    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 最小空闲连接数
        jedisPoolConfig.setMinIdle(5);
        // 最大连接数
        jedisPoolConfig.setMaxTotal(100);
        // 最大空闲连接数
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000L);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(false);
        jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000);
    }
    // 获取Jedis
    public static Jedis getJedis() {
        System.out.println("~~~~~~~开启Jedis~~~~~~~");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
    // 关闭Jedis
    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    // 从Redis中查询维度数据
    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        // 拼接key
        String redisKey = getRedisKey(tableName, id);
        // 根据key到Redis中获取维度数据
        String dimJsonStr = jedis.get(redisKey);
        // 如果获取到了
        if (StringUtils.isNotEmpty(dimJsonStr)) {
            // 缓存命中，将jsonStr解析为json对象并返回
            JSONObject dimJsonObj = JSONObject.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        return null;
    }

    // 拼接key的方法
    private static String getRedisKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    // 向Redis中放入维度数据
    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dimJsonObj) {
        // 拼接key
        String redisKey = getRedisKey(tableName, id);
        // setex：set expire，带过期的set，只缓存1天
        jedis.setex(redisKey,24 * 3600,dimJsonObj.toJSONString());
    }
    // 测试连接
    public static void main(String[] args) {
        Jedis jedis = getJedis();
        System.out.println(jedis.ping());
        closeJedis(jedis);
    }


    // 获取异步操作Redis的客户端
    public static StatefulRedisConnection<String,String> getAsyncRedisConnection(){
        System.out.println("~~~~~~~开启Redis异步客户端~~~~~~~");
        // 连接Redis的0号库
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/0");
        StatefulRedisConnection<String, String> redisConn = redisClient.connect();
        return redisConn;
    }
    // 关闭异步操作Redis的客户端
    public static void closeAsyncRedisConnection(StatefulRedisConnection<String,String> redisConn){
        System.out.println("~~~~~~~关闭Redis异步客户端~~~~~~~");
        if (redisConn != null && redisConn.isOpen()) {
            redisConn.close();;
        }
    }

    // 以异步方式从Redis中读取数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String,String> asyncRedisConn, String tableName, String id) {
        // 用表名和主键拼接Redis键
        String redisKey = getRedisKey(tableName, id);
        // 获取异步指令
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        try {
            // 根据Redis键获取值
            String dimJsonStr = asyncCommands.get(redisKey).get();
            // 如果值非空，就转化为json对象返回
            if (StringUtils.isNotEmpty(dimJsonStr)) {
                JSONObject dimJsonObj = JSONObject.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    // 以异步方式向Redis写入数据
    public static void writeDimAsync(StatefulRedisConnection<String,String> asyncRedisConn, String tableName, String id, JSONObject dimJsonObj) {
        // 拼接Redis键
        String redisKey = getRedisKey(tableName, id);
        // setex：set expire，带过期的set，只缓存1天
        asyncRedisConn.async().setex(redisKey, 24 * 3600, dimJsonObj.toJSONString());
    }
}
