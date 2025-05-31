package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.DimSinkFunction;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author Felix
 * @date 2024/9/28
 * 维度层Dim的代码开发
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、DimApp
 * 开发流程
 *      基本环境准备
 *      检查点设置
 *      从kafka的topic_db主题中读取数据
 *      对流中数据进行类型转换以及ETL    jsonStr->jsonObj
 *      ~~~~~~~~~~~~~~~~~~~主流~~~~~~~~~~~~~~~~~~~~~~~
 *      使用FlinkCDC读取配置表配置信息
 *      对配置流数据进行类型转换    jsonStr->TableProcessDim
 *      ~~~~~~~~~~~~~~~~~~~配置流~~~~~~~~~~~~~~~~~~~~~~~
 *      根据配置表中的配置信息到HBase中建表以及删除
 *      ~~~~~~~~~~~~~~~~~~~建表~~~~~~~~~~~~~~~~~~~~~~~
 *      广播配置流---broadcast(广播状态描述器对象)
 *      将主流业务数据和广播流配置数据进行关联---connect
 *      对关联后的数据进行处理---process
 *          class TableProcessFunction extends BroadcastProcessFunction{
 *              open:   将配置表配置信息预加载到程序中     解决主流数据先到，广播流数据后到的情况
 *              processElement: 处理主流数据
 *                  根据表名到广播状态以及configMap中获取对应的配置信息，如果配置不为空，说明是维度，封装为Tuple2向下游传递
 *                  在向下游传递数据前，过滤掉不需要传递的属性
 *                  在向下游传递数据前，补充了type属性
 *              processBroadcastElement: 处理广播数据
 *                  op=d     从广播状态以及configMap中删除对应的配置信息
 *                  op!=d    将配置信息更新到广播状态以及configMap中
 *          }
 *      将维度数据写到HBase
 *          class DimSinkFunction{
 *              invoke:
 *                  type=delete   从hbase表中删除一条数据
 *                  type!=delete  向hbase表中put一条数据
 *          }
 * 程序执行的流程  以修改base_trademark表中一条数据为例
 *      当程序启动的时候，会从配置表中加载配置信息到configMap以及广播状态中
 *      当表中数据被修改后，binlog会记录数据的变化
 *      maxwell会将binlog中变化的数据读取到并封装为json格式字符串发送到kafka的topic_db主题中
 *      dimApp从topic_db主题中读取数据并和配置流数据进行关联
 *      在对主流数据进行处理的时候，会根据配置表的配置信息判断是不是维度
 *      如果是维度的话，传递到下游，将数据同步到HBase
 *
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10002,4,"dim_app_group",Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对读取的数据进行类型转换并进行简单的ETL   String->JsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        //TODO 2.使用FlinkCDC从配置表中读取配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);

        //TODO 3.根据配置信息在HBase中进行建表或者删表
        tpDS = createHBaseTable(tpDS);

        //TODO 4.广播配置流---broadcast 将主流业务数据和广播流配置信息进行关联---connect  对关联后的数据进行处理---process(过滤出维度数据)
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(tpDS, jsonObjDS);

        //TODO 5.将维度数据同步到HBase对应的表中
        writeToHbase(dimDS);
    }

    private static void writeToHbase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new DimSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<TableProcessDim> tpDS, SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);


        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);


        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        dimDS.print();
        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    Connection hBaseConn = null;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hBaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tableProcessDim.getOp();
                        //获取操作的HBase表的名
                        String sinkTable = tableProcessDim.getSinkTable();
                        //获取HBase表的列族
                        String[] families = tableProcessDim.getSinkFamily().split(",");

                        if("d".equals(op)){
                            //说明从配置表中删除了一条配置信息   对应的应该从HBase中将这张表删除掉
                            HBaseUtil.dropHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("c".equals(op)||"r".equals(op)){
                            //说明从配置表中读取了一条配置或者向配置表中 添加了一条配置  对应的在HBase中将表创建
                            HBaseUtil.createHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,families);
                        }else{
                            //说明对配置表的配置信息进行了更新操作
                            //先删除表
                            HBaseUtil.dropHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            //再创建表
                            HBaseUtil.createHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,families);
                        }
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        //2.1 创建MySqlSource数据源对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall0422_config", "table_process_dim");
        //2.2 读取数据 封装为流
        DataStreamSource<String> myStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);


        //"op":"r": {"before":null,"after":{"source_table":"spu_info","sink_table":"dim_spu_info","sink_family":"info","sink_columns":"id,spu_name,description,category3_id,tm_id","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall0422_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1727504366510,"transaction":null}
        //"op":"c": {"before":null,"after":{"source_table":"a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1727504427000,"snapshot":"false","db":"gmall0422_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":15045809,"row":0,"thread":15,"query":null},"op":"c","ts_ms":1727504427825,"transaction":null}
        //"op":"u": {"before":{"source_table":"a","sink_table":"dim_a_b","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"after":{"source_table":"a","sink_table":"dim_a_b","sink_family":"info","sink_columns":"id,name,age","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1727504516000,"snapshot":"false","db":"gmall0422_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":15046562,"row":0,"thread":15,"query":null},"op":"u","ts_ms":1727504516670,"transaction":null}
        //"op":"d": {"before":{"source_table":"a","sink_table":"dim_a_b","sink_family":"info","sink_columns":"id,name,age","sink_row_key":"id"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1727504540000,"snapshot":"false","db":"gmall0422_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":15046952,"row":0,"thread":15,"query":null},"op":"d","ts_ms":1727504540879,"transaction":null}

        //myStrDS.print();

        //2.3.将读取到的配置信息进行类型转换    String->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = myStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        //为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取对配置变进行的操作的类型
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            //说明从配置表中删除了一条数据  应该before属性中获取删除前的配置信息
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            //说明从配置表中读取了一条数据或者添加以及修改了一条配置信息  应该after属性中获取最新的配置信息
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }

                        tableProcessDim.setOp(op);

                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

        //tpDS.print();
        return tpDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String db = jsonObj.getString("database");
                            String type = jsonObj.getString("type");
                            String data = jsonObj.getString("data");
                            if ("gmall0422".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2
                            ) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
        );

        //jsonObjDS.print();
        return jsonObjDS;
    }
}