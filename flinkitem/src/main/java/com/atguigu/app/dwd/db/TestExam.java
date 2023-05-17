package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: TestExam
 * Package: com.atguigu.app.dwd.db
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/17 8:50
 * @Version 1.2
 */
public class TestExam {
    public static void main(String[] args) {


        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905));

        //检查点
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


        //获取kafka topic_db数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("trade_order_detail"));

        Table testExamTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id, \n" +
                "    `data`['paper_id'] paper_id, \n" +
                "    `data`['user_id'] user_id, \n" +
                "    `data`['score'] score, \n" +
                "    `data`['duration_sec'] duration_sec, \n" +
                "    `data`['create_time'] create_time, \n" +
                "    `data`['submit_time'] submit_time\n" +
                "from topic_db\n" +
                "where `database` = 'edu-flink'\n" +
                "and `table` = 'test_exam'\n" +
                "and `type` = 'insert'\n");

        tableEnv.createTemporaryView("test_exam",testExamTable);


        Table testExamQuestionTable = tableEnv.sqlQuery("" +
                "select\n" +
                "   `data`['id']  id,\n" +
                "   `data`['exam_id']  exam_id,\n" +
                "   `data`['paper_id']  paper_id,\n" +
                "   `data`['question_id']  question_id,\n" +
                "   `data`['user_id']  user_id,\n" +
                "   `data`['answer']  answer,\n" +
                "   `data`['is_correct']  is_correct,\n" +
                "   `data`['score']  score,\n" +
                "   `data`['create_time']  create_time\n" +
                "from topic_db\n" +
                "where `database` = 'edu-flink'\n" +
                "and `table` = 'test_exam_question'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("test_exam_question",testExamQuestionTable);


        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    te.id,\n" +
                "    te.paper_id,\n" +
                "    te.user_id,\n" +
                "    te.score,\n" +
                "    te.duration_sec,\n" +
                "    te.create_time,\n" +
                "    te.submit_time,\n" +
                "    eq.question_id,\n" +
                "    eq.answer,\n" +
                "    eq.is_correct\n" +
                "from test_exam te\n" +
                "join test_exam_question eq\n" +
                "on te.id = eq.exam_id");
        tableEnv.createTemporaryView("result_table",resultTable);


        tableEnv.executeSql("" +
                "create table dwd_test_exam(\n" +
                "     id string,\n" +
                "     paper_id string,\n" +
                "     user_id string,\n" +
                "     score string,\n" +
                "     duration_sec string,\n" +
                "     create_time string,\n" +
                "     submit_time string,\n" +
                "     question_id string,\n" +
                "     answer string,\n" +
                "     is_correct string,\n" +
                "     PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+MyKafkaUtil.getUpsertKafkaSinkConnOption("dwd_test_exam"));



        tableEnv.executeSql("insert into dwd_test_exam select * from result_table");

    }
}
