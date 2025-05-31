package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/29
 * FlinkAPI程序的基类
 *      模板方法设计模式：在父类中定义完成某一个功能的核心算法骨架(步骤)，但是某些步骤在父类中没有办法去实现，需要延迟到子类中去完成
 *      好处:
 *          定义统一的模板
 *          在不改变父类核心算法骨架的前提下，每一个子类都可以有自己不同的实现
 *
 */
public abstract class BaseApp {
    public void start(int port, int parallelism, String ckAndGroupId, String topic){
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(parallelism);

        //TODO 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        CheckpointConfig cpConfig = env.getCheckpointConfig();
        //2.2 设置检查点超时时间
        cpConfig.setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
        cpConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        cpConfig.setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置状态后端 设置检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        cpConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck/" + ckAndGroupId);
        //2.6 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 3.从kafka主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String groupId = ckAndGroupId;
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, groupId);
        //3.3 读取数据 封装为流
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        //TODO 4.业务处理
        handle(env,kafkaStrDS);
        //TODO 5.提交作业
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) ;
}
