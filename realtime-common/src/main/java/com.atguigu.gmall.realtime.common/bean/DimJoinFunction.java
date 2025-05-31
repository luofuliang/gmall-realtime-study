package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @author Felix
 * @date 2024/10/14
 * 维度关联应该实现的接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj);

    String getTableName();

    String getRowKey(T obj) ;
}
