package com.atguigu;

import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2022-08-27 9:18
 */
public class Test10_NullKafka {
    public static void main(String[] args) throws Exception {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        env.addSource(KafkaUtil.getKafkaConsumer1("user_table","group2"))
                .print();

        env.execute();
    }
}
