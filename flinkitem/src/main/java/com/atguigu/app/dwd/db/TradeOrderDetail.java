package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: TradeOrderDetail
 * Package: com.atguigu.app.dwd.db
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/16 19:50
 * @Version 1.2
 */
public class TradeOrderDetail {


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


        //过滤订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select \n" +
                "     `data`['id'] id,\n" +
                "     `data`['course_id'] course_id,\n" +
                "     `data`['course_name'] course_name,\n" +
                "     `data`['order_id'] order_id,\n" +
                "     `data`['user_id'] user_id,\n" +
                "     `data`['origin_amount'] origin_amount,\n" +
                "     `data`['coupon_reduce'] coupon_reduce,\n" +
                "     `data`['final_amount'] final_amount,\n" +
                "     `data`['create_time'] create_time,\n" +
                "     `data`['update_time'] update_time,\n" +
                "      pt\n" +
                "from topic_db\n" +
                "where `database` = 'edu-flink'\n" +
                "and `table` = 'order_detail'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail",orderDetailTable);

        //过滤订单数据

        Table orderInfoTable = tableEnv.sqlQuery("" +
                "select\n" +
                "      `data`['id'] id,\n" +
                "      `data`['user_id'] user_id,\n" +
                "      `data`['origin_amount'] origin_amount,\n" +
                "      `data`['coupon_reduce'] coupon_reduce,\n" +
                "      `data`['final_amount'] final_amount,\n" +
                "      `data`['order_status'] order_status,\n" +
                "      `data`['trade_body'] trade_body,\n" +
                "      `data`['session_id'] session_id,\n" +
                "      `data`['province_id'] province_id,\n" +
                "      `data`['create_time'] create_time,\n" +
                "      `data`['expire_time'] expire_time,\n" +
                "      `data`['update_time'] update_time\n" +
                "from topic_db\n" +
                "where `database` = 'edu-flink'\n" +
                "and `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info",orderInfoTable);


        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "   od.id,\n" +
                "   od.course_id,\n" +
                "   od.course_name,\n" +
                "   od.order_id,\n" +
                "   od.user_id,\n" +
                "   od.coupon_reduce,\n" +
                "   od.final_amount,\n" +
                "   od.create_time,\n" +
                "   oi.origin_amount,\n" +
                "   oi.order_status,\n" +
                "   oi.trade_body,\n" +
                "   oi.session_id,\n" +
                "   oi.province_id,\n" +
                "   oi.expire_time\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id=oi.id");

        tableEnv.createTemporaryView("result_table",resultTable);

        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "     `id` string,\n" +
                "     `course_id` string,\n" +
                "     `course_name` string,\n" +
                "     `order_id` string,\n" +
                "     `user_id` string,\n" +
                "     `coupon_reduce` string,\n" +
                "     `final_amount` string,\n" +
                "     `create_time` string,\n" +
                "     `origin_amount` string,\n" +
                "     `order_status` string,\n" +
                "     `trade_body` string,\n" +
                "     `session_id` string,\n" +
                "     `province_id` string,\n" +
                "     `expire_time` string,\n" +
                "     PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+MyKafkaUtil.getUpsertKafkaSinkConnOption("dwd_trade_order_detail"));

        tableEnv.executeSql("insert into dwd_trade_order_detail select * from result_table");


    }

}
