package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Dwd02_CounrseComment
 * Package: com.atguigu.app.dwd.db
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/5/16 20:25
 * @Version 1.0
 */
public class TrafficCounrseReview {
    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //TODO 2.使用FlinkSQL方式读取Kafka topic_db 主题数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_course_review_221109"));
        //TODO 3.过滤出评价数据
        Table CourseReview = tableEnv.sqlQuery("" +
                "select\n" +
                "`data`['id'] id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['course_id'] course_id,\n" +
                "`data`['review_txt'] review_txt,\n" +
                "`data`['review_stars'] review_stars,\n" +
                "`data`['create_time'] create_time\n" +
                "from topic_db\n" +
                "where `database`='edu-flink'\n" +
                "and `table`='review_info'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("course_review",CourseReview);

        //TODO 4.创建Kafka DWD层评价事实表主题

        tableEnv.executeSql("" +
                "create table dwd_course_review(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "review_txt string,\n" +
                "review_stars string,\n" +
                "create_time string\n" +
                ")" + MyKafkaUtil.getKafkaSinkConnOption("dwd_course_review"));

        //TODO 5.插入数据
        tableEnv.executeSql("insert into dwd_course_review select * from course_review");


    }
}
