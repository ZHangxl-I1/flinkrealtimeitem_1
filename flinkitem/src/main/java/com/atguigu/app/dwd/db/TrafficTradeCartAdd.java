package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Dwd02_TradeCartAdd
 * Package: com.atguigu.app.dwd.db
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/5/16 19:48
 * @Version 1.0
 */
public class TrafficTradeCartAdd {
    public static void main(String[] args) throws Exception {
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
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_cart_add_221109"));
        //TODO 3.过滤出加购数据
        Table cartInfoTable = tableEnv.sqlQuery("" +
                "select\n" +
                "`data`['id'] id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['course_id'] course_id,\n" +
                "`data`['course_name'] course_name,\n" +
                "`data`['cart_price'] cart_price,\n" +
                "`data`['session_id'] session_id,\n" +
                "`data`['create_time'] create_time,\n" +
                "`data`['sold'] sold,\n" +
                " `pt`\n" +
                "from topic_db\n" +
                "where `database`='edu-flink'\n" +
                "and `table`='cart_info'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("cart_info", cartInfoTable);

        //TODO 4.创建Kafka DWD层加购事实表主题
        tableEnv.executeSql("" +
                "create table  dwd_cart_info(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    course_id string,\n" +
                "    course_name string,\n" +
                "    cart_price string,\n" +
                "    session_id string,\n" +
                "    create_time string,\n" +
                "    sold string  ,\n" +
                "    pt   TIMESTAMP_LTZ(3)\n" +
                ")" + MyKafkaUtil.getKafkaSinkConnOption("dwd_trade_cart_add"));
        //TODO 5.插入数据
        tableEnv.executeSql("insert into dwd_cart_info select * from cart_info");




    }

}
