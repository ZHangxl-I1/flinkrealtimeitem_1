package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: Dwd31_TestPaper
 * Package: com.atguigu.app.dwd.db
 * Description: 学习域 测验试卷 粒度 事务事实表
 *
 * @Author NoahZhang
 * @Create 2023/5/17 9:03
 * @Version 1.0
 */
public class Dwd31_TestPaper {

    public static void main(String[] args) throws Exception {

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

        //TODO 3.过滤出测验表 test_exam
        // 云主机的业务数据库名 edu-flink
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
                "where `database`='edu-flink' " +
                "and `table`='test_exam' " +
                "and `type`='insert'");
        //创建虚拟视图
        tableEnv.createTemporaryView("test_exam", testExamTable);

        //测试OK
//        tableEnv.sqlQuery("select * from test_exam").execute().print();

        //TODO 4.读取MySQL test_paper表。FlinkSQL Kafka连接器不可行，因为维度表没有topic_db数据过来，用JDBC SQL Connector，关联时用Lookup Join，数据类型需要同MySql中test_paper的一致。
        tableEnv.executeSql("" +
                "create table test_paper( " +
                "    id bigint, " +
                "    paper_title string, " +
                "    course_id bigint, " +
                "    primary key (id) not enforced " +
                ")  " +
                "with( " +
                "  'connector' = 'jdbc', " +
                "  'url' = 'jdbc:mysql://hadoop102:3306/edu-flink', " +
                "  'table-name' = 'test_paper', " +
                "  'username' = 'root', " +
                "  'password' = '000000' " +
                ")");

        //测试OK
        tableEnv.sqlQuery("select * from test_paper").execute().print();

        //TODO 5.两表关联， Lookup Join 通常在 Flink SQL 表和外部系统查询结果关联时使用。
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    test_exam.id, " +
                "    test_exam.paper_id, " +
                "    test_exam.user_id, " +
                "    test_exam.score, " +
                "    test_exam.duration_sec, " +
                "    test_exam.create_time, " +
                "    test_paper.paper_title, " +
                "    test_paper.course_id " +
                "from test_exam " +
                "join test_paper FOR SYSTEM_TIME AS OF test_exam.pt " +
                "on cast(test_exam.paper_id as bigint)=test_paper.id");

        tableEnv.createTemporaryView("result_table",resultTable);

//        测试OK
//        tableEnv.sqlQuery("select * from result_table").execute().print();

        //TODO 6.使用kafka创建DWD层测验试卷主题(非upsert-kafka，不需要主键)，course_id的数据类型需要是bigint
        tableEnv.executeSql("" +
                "create table dwd_test_paper(  " +
                "    id string,  " +
                "    paper_id string,  " +
                "    user_id string,  " +
                "    score string,  " +
                "    duration_sec string,  " +
                "    create_time string,  " +
                "    paper_title string,  " +
                "    course_id bigint  " +
                ")" + MyKafkaUtil.getKafkaSinkConnOption("dwd_test_paper"));

        //TODO 7.写出数据
        tableEnv.executeSql("insert into dwd_test_paper select * from result_table");


    }
}
