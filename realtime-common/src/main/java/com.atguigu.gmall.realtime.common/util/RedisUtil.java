package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Felix
 * @date 2024/10/14
 * 操作Redis的工具类
 * 旁路缓存
 *      思路：先从缓存中查询维度数据，如果从缓存中获取到了要关联的维度，那么直接将其作为返回值返回(缓存命中)
 *           如果从缓存中没有获取到要关联的维度，那么发送请求到HBase中获取对应的维度数据，并将其放到缓存
 *           中缓存起来，方便下次查询使用
 *      缓存产品选型
 *          状态          性能非常好，但是维护性差
 *          Redis        性能不错，维护性好      √
 *      关于Redis的一些配置
 *          key:        维度表名:主键值    例如：   dim_base_trademark:2
 *          type:       String
 *          expire:     1day    避免冷数据常驻内存，给内存带来压力
 *          注意:       如果维度数据发生了变化，需要将缓存的数据清除掉
 */
public class RedisUtil {

    private static JedisPool jedisPool;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);

        jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000);
    }

    //获取Jedis
    public static Jedis getJedis(){
        System.out.println("~~~创建Jedis~~~");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
    //关闭Jedis
    public static void closeJedis(Jedis jedis){
        System.out.println("~~~关闭Jedis~~~");
        if(jedis != null){
            jedis.close();
        }
    }

    //获取支持异步操作Redis的客户端连接对象
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        System.out.println("~~~获取异步操作Redis的客户端~~~");
        StatefulRedisConnection<String, String> asyncRedisConn = RedisClient.create("redis://hadoop102:6379/0").connect();
        return asyncRedisConn;
    }
    //关闭支持异步操作Redis的客户端连接对象
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> asyncRedisConn) {
        System.out.println("~~~关闭异步操作Redis的客户端~~~");
        if(asyncRedisConn != null && asyncRedisConn.isOpen()){
            asyncRedisConn.close();
        }
    }



    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
        closeJedis(jedis);
    }

    //从Redis中读取维度数据
    public static JSONObject readDim(Jedis jedis,String tableName,String id){
        //拼接key
        String key = getKey(tableName,id);
        //根据key到Redis中获取对应的维度数据
        String dimJsonStr = jedis.get(key);
        //判断是否缓存命中
        if(StringUtils.isNotEmpty(dimJsonStr)){
            //缓存命中
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        return null;
    }

    public static String getKey(String tableName, String id) {
        return tableName + ":" + id ;
    }
    //向Redis中放维度数据
    public static void writeDim(Jedis jedis,String tableName,String id,JSONObject dimJsonObj){
        //拼接key
        String key = getKey(tableName, id);
        //将维度数据缓存到Redis中，并设置缓存的失效时间为1day
        jedis.setex(key,24 * 3600,dimJsonObj.toJSONString());
    }

    //以异步的方式从Redis中读取维度数据
    public static JSONObject readDimAsync( StatefulRedisConnection<String, String> asyncRedisConn,String tableName,String id){
        //拼接key
        String key = getKey(tableName, id);
        try {
            String dimJsonStr = asyncRedisConn.async().get(key).get();
            if(StringUtils.isNotEmpty(dimJsonStr)){
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    //以异步的方式向Redis中放维度数据
    public static void writeDimAsync(StatefulRedisConnection<String, String> asyncRedisConn,String tableName,String id,JSONObject dimJsonObj){
        //拼接key
        String key = getKey(tableName, id);
        asyncRedisConn.async().setex(key,24*3600, dimJsonObj.toJSONString());
    }
}
