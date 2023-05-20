package com.atguigu.app.dws;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: Dwd32_TestExamQuestion
 * Package: org.st.app.dwd.db
 * Description: 粒度为一个question_id的考试答题，test_exam_question作为主表
 *
 * @Author NoahZH
 * @Create 2023/5/18 0:07
 * @Version 1.0
 */

//数据流：web/app -> Mysql -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程 序：Mock -> Mysql -> Maxwell -> Kafka(ZK) -> Dwd32_TestExamQuestion -> Kafka(ZK)
//业务数据库edu -> edu-flink
public class Dwd32_TestExamQuestion {
    public static void main(String[] args) {

        //TODO 1.获取执行环境   注意设置TTL
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        tableEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "5s");
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

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
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/edu/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取Kafka topic_db 主题数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_test_paper"));

        //TODO 3.过滤出 test_exam_question
        Table testExamQuestionTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['exam_id'] exam_id,\n" +
                "    `data`['paper_id'] paper_id,\n" +
                "    `data`['question_id'] question_id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['answer'] answer,\n" +
                "    `data`['is_correct'] is_correct,\n" +
                "    `data`['score'] score,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['update_time'] update_time,\n" +
                "    `data`['deleted'] deleted\n" +
                "from topic_db\n" +
                "where `database`='edu'\n" +
                "and `table`='test_exam_question'\n" +
                "and `type`='insert'");
        //创建虚拟视图
        tableEnv.createTemporaryView("test_exam_question",testExamQuestionTable);

        //TODO 4.过滤出测验表 test_exam
        Table testExamTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['paper_id'] paper_id, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['score'] score, " +
                "    `data`['duration_sec'] duration_sec, " +
                "    `data`['create_time'] create_time, " +
                "    `pt` " +
                "from topic_db " +
                "where `database`='edu' " +
                "and `table`='test_exam' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("test_exam", testExamTable);


        //TODO 5.两表关联
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    t1.id,\n" +
                "    t1.exam_id,\n" +
                "    t1.paper_id,\n" +
                "    t1.question_id,\n" +
                "    t1.user_id,\n" +
                "    t1.answer,\n" +
                "    t1.is_correct,\n" +
                "    t1.score question_score,\n" +
                "    t2.score paper_score,\n" +
                "    t2.duration_sec,\n" +
                "    t2.create_time\n" +
                "from test_exam_question t1\n" +
                "join test_exam t2\n" +
                "on t1.exam_id=t2.id");
        tableEnv.createTemporaryView("result_table",resultTable);

        //TODO 6.使用kafka创建DWD层主题 Dwd32_TestExamQuestion
        tableEnv.executeSql("" +
                "create table dwd_test_exam_question(\n" +
                "    id string,\n" +
                "    exam_id string,\n" +
                "    paper_id string,\n" +
                "    question_id string,\n" +
                "    user_id string,\n" +
                "    answer string,\n" +
                "    is_correct string,\n" +
                "    question_score string,\n" +
                "    paper_score string,\n" +
                "    duration_sec string,\n" +
                "    create_time string\n" +
                ")" + MyKafkaUtil.getKafkaSinkConnOption("dwd_test_exam_question"));

        //TODO 7.写出
        tableEnv.executeSql("insert into dwd_test_exam_question select * from result_table");

    }

}
