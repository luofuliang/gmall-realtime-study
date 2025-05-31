package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * @author Felix
 * @date 2024/9/29
 * 对关联后的数据进行处理-过滤出维度数据
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    //定义一个Map集合，用于存放预加载的配置信息
    Map<String, TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表中的配置信息预加载到程序中
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mysqlConnection, "select * from " +
                "gmall0422_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeMysqlConnection(mysqlConnection);
    }

    //对主流业务数据进行处理
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim,
            Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx,
                               Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        //获取当前处理的业务数据库表的表名
        String table = jsonObj.getString("table");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据表名到广播状态中获取对应的配置信息
        TableProcessDim tableProcessDim = null;


        //判断当前处理的是不是维度数据
        if ((tableProcessDim = broadcastState.get(table)) != null
                || (tableProcessDim = configMap.get(table)) != null) {
            //说明当前处理的是维度数据   将这条数据data部分以及这条维度对应的配置对象封装为二元组向下游传递
            //{"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"123","id":1}
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            //在向下游传递数据前，过滤掉不需要传递的属性
            //{"tm_name":"Redmi","id":1}
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumn(dataJsonObj, sinkColumns);

            //在向下游传递数据前，补充type属性(type:对业务数据库维度表进行了什么操作)
            String type = jsonObj.getString("type");
            dataJsonObj.put("type", type);
            //{"tm_name":"Redmi111","id":1,"type":"update"}
            out.collect(Tuple2.of(dataJsonObj, tableProcessDim));
        }

    }

    //对广播流中的配置信息进行处理
    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject,
            TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject,
            TableProcessDim>> out) throws Exception {
        //获取对配置表进行操作的类型
        String op = tableProcessDim.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String sourceTable = tableProcessDim.getSourceTable();
        if ("d".equals(op)) {
            //说明从配置表中删除了一条配置信息  将这条配置从广播状态中删除掉
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            //说明对配置表进行了读取、添加、修改操作   将这条配置更新到广播状态中
            broadcastState.put(sourceTable, tableProcessDim);
            configMap.put(sourceTable, tableProcessDim);
        }
    }

    //过滤掉不需要传递的字段
    //dataJsonObj: {"tm_name":"Redmi111","create_time":"2021-12-14 00:00:00","logo_url":"123","id":1,"type":"update"}
    //sinkColumns: id,tm_name
    private void deleteNotNeedColumn(JSONObject dataJsonObj, String sinkColumns) {
        //获取要保留的字段的集合
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));

    }
}
