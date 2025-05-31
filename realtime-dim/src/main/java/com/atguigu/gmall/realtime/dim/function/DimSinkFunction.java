package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @author Felix
 * @date 2024/9/29
 * 将流中数据同步到HBase
 */
public class DimSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    Connection hBaseConn = null;
    Jedis jedis = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseConn = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hBaseConn);
        RedisUtil.closeJedis(jedis);
    }
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup2, Context context) throws Exception {
        //将流中数据同步到Hbase对应的表中
        //{"tm_name":"Redmi222","id":1,"type":"update"}
        JSONObject dataJsonObj = tup2.f0;
        TableProcessDim tableProcessDim = tup2.f1;

        //获取对业务数据库维度表进行的操作的类型
        String type = dataJsonObj.getString("type");
        dataJsonObj.remove("type");

        String sinkTable = tableProcessDim.getSinkTable();
        String rowKey = dataJsonObj.getString(tableProcessDim.getSinkRowKey());
        if("delete".equals(type)){
            //说明对业务数据库维度表进行了删除操作   那么在HBase对应的维度表中也需要删除一条数据
            HBaseUtil.delRow(hBaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else{
            //说明对业务数据库维度表进行了insert、update、bootstrap-insert操作   那么在HBase对应的维度表中需要执行put操作
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamily,rowKey,dataJsonObj);
        }

        //如果维度表数据发生了变化，将Redis中缓存的数据清除掉
        if("delete".equals(type)||"update".equals(type)){
            System.out.println("~~~从Redis中删除维度数据~~~");
            //拼接key
            String key = RedisUtil.getKey(sinkTable, rowKey);
            jedis.del(key);
        }
    }
}
