package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2022-08-23 14:25
 */
public class Test09_TimeFunc {
    public static void main(String[] args) {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // 填写s的时间戳 转换为字符串显示
        tableEnv.sqlQuery("select TO_TIMESTAMP_LTZ(1645429515,'yyyy-MM-dd HH:mm:ss')").execute().print();

        // 填写字符串转换为时间戳
        tableEnv.sqlQuery("select UNIX_TIMESTAMP('2022-02-21 15:45:15','yyyy-MM-dd')").execute().print();
        tableEnv.sqlQuery("select UNIX_TIMESTAMP('2022-02-21 15:45:15','yyyy-MM-dd')").execute().print();



    }
}
