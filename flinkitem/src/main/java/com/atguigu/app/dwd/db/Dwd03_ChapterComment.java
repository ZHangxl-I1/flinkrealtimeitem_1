package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Dwd03_ChapterComment
 * Package: com.atguigu.app.dwd.db
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/5/16 20:50
 * @Version 1.0
 */
public class Dwd03_ChapterComment {
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
        //TODO 3.过滤出章节评价数据
        Table ChapterComment = tableEnv.sqlQuery("" +
                "select " +
                "`data`['id'] id, " +
                "`data`['user_id'] user_id, " +
                "`data`['chapter_id'] chapter_id, " +
                "`data`['course_id'] course_id, " +
                "`data`['comment_txt'] comment_txt, " +
                "`data`['create_time'] create_time " +
                "from topic_db " +
                "where `database`='edu-flink' " +
                "and `table`='comment_info' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("chapter_info",ChapterComment);
        //TODO 4.创建Kafka DWD层章节评价事实表主题
        tableEnv.executeSql("" +
                "create table  dwd_chapter_commnet( " +
                "id string, " +
                "user_id string, " +
                "chapter_id string, " +
                "course_id string, " +
                "comment_txt string, " +
                "create_time string" +
                ")"+MyKafkaUtil.getKafkaSinkConnOption("dwd_chapter_commnet"));
        //TODO 5.插入数据
        tableEnv.executeSql("insert into dwd_chapter_commnet select * from chapter_info");


    }
}
