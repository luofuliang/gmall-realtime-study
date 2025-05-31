package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Felix
 * @date 2024/9/29
 * 从MySQL数据表中查询数据
 */
public class JdbcUtil {
    public static Connection getMysqlConnection() throws Exception {
        //注册驱动
        Class.forName(Constant.MYSQL_DRIVER);
        //建立连接
        Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        return conn;
    }

    public static void closeMysqlConnection(Connection conn) throws Exception {
        if(conn != null && !conn.isClosed()){
            conn.close();
        }
    }


    public static <T>List<T> queryList(Connection conn,String sql,Class<T> clz) throws Exception{
        return queryList(conn,sql,clz,false);
    }
    /**
     *  通用从MySQL数据库表中查询数据的方法
     * @param conn                      MySQL连接对象
     * @param sql                       要执行的SQL语句
     * @param clz                       将一条查询结果封装为什么类型
     * @param isUnderLineToCamel        是否要将下划线转换为驼峰命名
     */
    public static <T>List<T> queryList(Connection conn,String sql,Class<T> clz,boolean isUnderLineToCamel) throws Exception {
        List<T> resList = new ArrayList<>();
        //获取数据库操作对象
        PreparedStatement ps = conn.prepareStatement(sql);
        //执行SQL语句
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        //处理结果集
        while (rs.next()){
            //定义一个对象 用于封装查询结果
            T obj = clz.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                if(isUnderLineToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.setProperty(obj,columnName,columnValue);
                resList.add(obj);
            }
        }
        //释放资源
        if(rs != null){
            rs.close();
        }
        if(ps != null){
            ps.close();
        }

        return resList;
    }
}
