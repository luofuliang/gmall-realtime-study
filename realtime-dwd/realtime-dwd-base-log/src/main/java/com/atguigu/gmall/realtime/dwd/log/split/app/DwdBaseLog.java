package com.atguigu.gmall.realtime.dwd.log.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @auther Aliang
 * @date 2025-05-30 - 0:14
 * @Descirption
 */
public class DwdBaseLog extends BaseApp {

    private final String ERR = "err";
    private final String START = "start";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "di_data_base", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        //TODO 1. 对流中数据进行类型转换 并做简单的ETL 脏数据放到侧流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);


        //TODO 2. 使用Flink的状态编程对新老访客标记进行修复
        SingleOutputStreamOperator<JSONObject> fixedDS = fixNewAndOld(jsonObjDS);


        //TODO 3. 分流 将不同类型的日志放到不同的流中
        Map<String, DataStream> dsMap = splitStream(fixedDS);

        //TODO 4. 将不同流的数据写到kafka 的不同主题中
        writeToKafka(dsMap);
    }

    private void writeToKafka(Map<String, DataStream> dsMap) {
        dsMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        dsMap
                .get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        dsMap
                .get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        dsMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        dsMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private Map<String, DataStream> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        //3.1 定义侧输出流标签 错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流 页面日志-主流
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };

        //3.2 分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx,
                                       Collector<String> out) throws Exception {
                //~~~错误日志~~~
                JSONObject errJsonObj = jsonObj.getJSONObject("err");
                if (errJsonObj != null) {
                    //将错误日志输出到错误侧输出流
                    ctx.output(errTag, jsonObj.toJSONString());
                    jsonObj.remove("err");
                }

                JSONObject startJsonObj = jsonObj.getJSONObject("start");
                if (startJsonObj != null) {
                    //~~~启动日志~~~
                    //将启动日志输出到启动侧输出流
                    ctx.output(startTag, jsonObj.toJSONString());
                } else {
                    //~~~页面日志~~~
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");

                    //~~~曝光日志~~~
                    JSONArray displaysArr = jsonObj.getJSONArray("displays");
                    if (displaysArr != null && displaysArr.size() > 0) {
                        for (int i = 0; i < displaysArr.size(); i++) {
                            JSONObject displayJsonObj = displaysArr.getJSONObject(i);
                            // 创建一个新的对象，封装曝光日志
                            JSONObject newDisplayJsonObj = new JSONObject();
                            newDisplayJsonObj.put("common", commonJsonObj);
                            newDisplayJsonObj.put("page", pageJsonObj);
                            newDisplayJsonObj.put("display", displayJsonObj);
                            newDisplayJsonObj.put("ts", ts);
                            ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                        }
                        jsonObj.remove("displays");
                    }

                    //~~~动作日志~~~
                    JSONArray actionsArr = jsonObj.getJSONArray("actions");
                    if (actionsArr != null && actionsArr.size() > 0) {
                        for (int i = 0; i < actionsArr.size(); i++) {
                            JSONObject actionJsonObj = actionsArr.getJSONObject(i);
                            JSONObject newActionJsonObj = new JSONObject();
                            newActionJsonObj.put("common", commonJsonObj);
                            newActionJsonObj.put("page", pageJsonObj);
                            newActionJsonObj.put("action", actionJsonObj);
                            newActionJsonObj.put("ts", ts);
                            ctx.output(actionTag, newActionJsonObj.toJSONString());
                        }
                        jsonObj.remove("actions");
                    }

                    //将页面日志输出到主流
                    out.collect(jsonObj.toJSONString());
                }
            }
        });

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        Map<String, DataStream> dsMap = new HashMap<>();
        dsMap.put(ERR,errDS);
        dsMap.put(START,startDS);
        dsMap.put(DISPLAY,displayDS);
        dsMap.put(ACTION,actionDS);
        dsMap.put(PAGE,pageDS);
        return dsMap;
    }

    private static SingleOutputStreamOperator<JSONObject> fixNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        //2.1 按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS =
                jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //2.2 修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject,
                JSONObject>() {
            // 声明状态
            private transient ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState",
                        String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj,
                                       KeyedProcessFunction<String, JSONObject, JSONObject>.Context context,
                                       Collector<JSONObject> collector) throws Exception {
                String isNew = jsonObj.getJSONObject("common").getString("is_new");
                // 从状态中获取上次访问日期
                String lastVisitDate = lastVisitDateState.value();
                // 获取当前日期
                Long ts = jsonObj.getLong("ts");
                String curDate = DateFormatUtil.tsToDate(ts);

                if ("1".equals(isNew)) {
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        lastVisitDateState.update(curDate);
                    } else {
                        if (!lastVisitDate.equals(curDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        long yesterdayTs = ts - 24 * 60 * 60 * 1000;
                        String yesterday = DateFormatUtil.tsToDate(yesterdayTs);
                        lastVisitDateState.update(yesterday);
                    }
                }
                collector.collect(jsonObj);
            }
        });
        return fixedDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        //1.1 定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };

        //1.2 转换和清理
        SingleOutputStreamOperator<JSONObject> jsonObjDS =
                kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context,
                                               Collector<JSONObject> collector) throws Exception {
                        // 标准json，则向下游传递
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            collector.collect(jsonObj);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        // 脏数据，则放到侧输出流中
                        context.output(dirtyTag, jsonStr);
                    }
                });
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);

        //1.3 将脏数据写到kafka 主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DIRTY);
        dirtyDS.sinkTo(kafkaSink);
        return jsonObjDS;
    }
}














