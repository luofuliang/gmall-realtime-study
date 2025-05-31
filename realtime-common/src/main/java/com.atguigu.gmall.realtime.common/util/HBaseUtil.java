package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author Felix
 * @date 2024/9/28
 * 操作HBase的工具类
 */
public class HBaseUtil {
    //获取连接
    public static Connection getHBaseConnection(){
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            Connection connection = ConnectionFactory.createConnection(conf);
            return connection;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //关闭连接
    public static void closeHBaseConnection(Connection hbaseConn){
        if(hbaseConn != null && !hbaseConn.isClosed()){
            try {
                hbaseConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //获取支持异步操作的连接对象
    public static AsyncConnection getHBaseAsyncConnection(){
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        try {
            AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    //关闭支持异步操作的连接对象
    public static void closeHBaseAsyncConnection(AsyncConnection asyncConnection){
        if(asyncConnection != null && !asyncConnection.isClosed()){
            try {
                asyncConnection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    /**
     * 在HBase中创建表
     * @param hbaseConn     hbase连接对象
     * @param namespace     表空间
     * @param tableName     表名
     * @param families      列族
     */
    public static void createHBaseTable(Connection hbaseConn,String namespace,String tableName,String ... families){
        if(families.length < 1){
            System.out.println("在HBase中建表至少需要一个列族");
            return;
        }
        try (Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(admin.tableExists(tableNameObj)){
                System.out.println("表空间"+namespace+"下的表"+tableName+"已经存在");
                return;
            }

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表空间"+namespace+"下的表"+tableName+"创建成功~");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //从HBase中删除表
    public static void dropHBaseTable(Connection hbaseConn,String namespace,String tableName){
        try (Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(!admin.tableExists(tableNameObj)){
                System.out.println("表空间"+namespace+"下的表"+tableName+"不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("表空间"+namespace+"下的表"+tableName+"删除成功~");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 向Hbase表中put数据
     * @param hbaseConn         hbase连接对象
     * @param namespace         表空间
     * @param tableName         表名
     * @param family            列族
     * @param rowKey            rowkey
     * @param jsonObj           向put到hbase中的 列-值
     */
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String family, String rowKey, JSONObject jsonObj){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columnNames = jsonObj.keySet();
            for (String columnName : columnNames) {
                String columnValue = jsonObj.getString(columnName);
                if(StringUtils.isNotEmpty(columnValue)){
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));
                }
            }
            table.put(put);
            System.out.println("向表空间"+namespace+"下的表"+tableName+",put数据"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //从Hbase表中删除
    public static void delRow(Connection hbaseConn, String namespace, String tableName,  String rowKey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间"+namespace+"下的表"+tableName+"删除数据"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //根据维度的主键从HBase表中查询一条数据
    public static <T>T getRow(Connection hbaseConn, String namespace, String tableName,  String rowKey,Class<T> clz, boolean... isUnderlineToCamel){
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            //获取当前rowkey对应的所有单元格(一行中的所有列)
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                //定义一个对象 用于封装遍历出来的结果
                T obj = clz.newInstance();
                //对所有列进行遍历
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (defaultIsUToC){
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    //给对象的属性赋值
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                return obj;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    //以异步的方式从HBase中根据RowKey获取对应的维度对象
    public static JSONObject readDimAsync(AsyncConnection asyncHbaseConn,String namespace, String tableName,  String rowKey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncHbaseConn.getTable(tableNameObj);
        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                //定义一个JSON对象，用于封装查询出来的一行数据
                JSONObject dimJsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    dimJsonObj.put(columnName,columnValue);
                }
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }


    public static void main(String[] args) {
        Connection hBaseConn = getHBaseConnection();
        System.out.println(getRow(hBaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", "1", JSONObject.class));
        closeHBaseConnection(hBaseConn);
    }
}
