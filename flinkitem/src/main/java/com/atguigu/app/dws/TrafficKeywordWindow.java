package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordBean;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Trasdd
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/18 14:03
 * @Version 1.2
 */
public class TrafficKeywordWindow {
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


        //使用FlinkSql获取dwd层页面数据，同时提取时间戳，生成WaterMark

        tableEnv.executeSql("" +
                "create table page_view(\n" +
                "`common` map<string,string>,\n" +
                "`page` map<string,string>,\n" +
                "`ts` BIGINT,\n" +
                "`rt` as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "WATERMARK FOR rt AS rt - INTERVAL '2' SECOND\n" +
                ")"+ MyKafkaUtil.getKafkaSoourceConnOption("dwd_traffic_page_log","traffic_keyword_page_view"));


        //TODO 3.过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("" +
                "select\n" +
                "   page['item'] item,\n" +
                "   rt\n" +
                "from page_view\n" +
                "where page['item'] is not null\n" +
//                "and page['last_page_id']='search'\n" +
                "and page['item_type']='keyword'");
        tableEnv.createTemporaryView("filter_table",filterTable);

        //注册UDTF函数
        tableEnv.createTemporarySystemFunction("split_func", SplitFunction.class);


        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT \n" +
                "    word,\n" +
                "    rt \n" +
                "FROM filter_table, LATERAL TABLE(split_func(item))\n");

        tableEnv.createTemporaryView("split_table",splitTable);


        //分组开窗聚合

        Table resultTable = tableEnv.sqlQuery("" +
                "SELECT \n" +
                "    DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    word keyword, \n" +
                "    count(*) keyword_count,\n" +
                "    UNIX_TIMESTAMP() ts\n" +
                "FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(rt), INTERVAL '10' seconds))\n" +
                "  GROUP BY word, window_start, window_end");

        //将动态表转换成流写入clickhouse
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);

        keywordBeanDataStream.print();

        keywordBeanDataStream.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_keyword_page_view_window values (?,?,?,?,?)"));



        env.execute("TrafficKeywordWindow");

    }
}
