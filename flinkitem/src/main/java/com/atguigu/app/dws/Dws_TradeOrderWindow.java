package com.atguigu.app.dws;

/*
1.4.3 交易综合统计
需求说明如下
统计周期	统计粒度	指标	说明
当日	---	下单总额	略
当日	---	下单人数	略
当日	---	下单次数	略

--ClickHouse建表语句如下：
drop table if exists dws_trade_order_count_window;
create table if not exists dws_trade_order_count_window
(
    stt                           DateTime COMMENT '窗口起始时间',
    edt                           DateTime COMMENT '窗口结束时间',
    total_amount Decimal(38, 20) COMMENT '下单总额',
    user_count    UInt64 COMMENT '下单人数',
    order_count    UInt64 COMMENT '下单次数',
    ts                            UInt64 COMMENT '时间戳'
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt);

主类 TradeOrderCountBean

* */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradeOrderCountBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.time.Duration;

public class Dws_TradeOrderWindow {

    public static void main(String[] args) throws Exception {


        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        //TODO 2.读取KafkaDWD层下单明细主题数据
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("dwd_trade_order_detail", "trade_order_window"), WatermarkStrategy.noWatermarks(), "kafka-source");


        //TODO 3.转换为JSON
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //{"id":"90329","course_id":"403","course_name":"大数据项目之电商数仓2.0","order_id":"81443","user_id":"1030","coupon_reduce":"0.0","final_amount":"200.0","create_time":"2023-05-19 22:52:41","origin_amount":"200.0","order_status":"1001","trade_body":"大数据项目之电商数仓2.0等1件商品","session_id":"b83e4c5f-c7c3-4cd0-99a8-4d6537b73b5b","province_id":"12","expire_time":"2023-05-19 23:07:41"}
        //TODO 4.提取时间戳生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("create_time");
            }
        }));

        //TODO 5.去重并转换为 TradeOrderCountBean
        SingleOutputStreamOperator<TradeOrderCountBean> beanDS = jsonObjWithWMDS.keyBy(json->json.getString("order_id")).flatMap(new RichFlatMapFunction<JSONObject, TradeOrderCountBean>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(org.apache.flink.api.common.time.Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("cart-state", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeOrderCountBean> out) throws Exception {

                //取出状态数据以及当前数据日期
                String lastDt = valueState.value();
                String curDt = value.getString("create_time").split(" ")[0];

                if (lastDt == null || !lastDt.equals(curDt)) {
                    valueState.update(curDt);
                    out.collect(new TradeOrderCountBean("", "", value.getBigDecimal("final_amount"), 1L, 1L, null));
                } else {
                    out.collect(new TradeOrderCountBean("", "", value.getBigDecimal("final_amount"), 0L, 1L, null));
                }

            }
        });


        //TODO 6.分组，开窗，聚合
        SingleOutputStreamOperator<TradeOrderCountBean> resultDS = beanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderCountBean>() {
                    @Override
                    public TradeOrderCountBean reduce(TradeOrderCountBean value1, TradeOrderCountBean value2) throws Exception {
                        value1.setTotalAmount(value1.getTotalAmount().add(value2.getTotalAmount()));
                        value1.setUserCount(value1.getUserCount() + value2.getUserCount());
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());

                        return value1;
                    }
                }, new AllWindowFunction<TradeOrderCountBean, TradeOrderCountBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderCountBean> values, Collector<TradeOrderCountBean> out) throws Exception {

                        TradeOrderCountBean next = values.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        out.collect(next);
                    }
                });

        //TODO 8.将数据写出
        resultDS.print("resultDS>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_order_count_window values(?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("Dws10_TradeProvinceOrderWindow");

    }
}
