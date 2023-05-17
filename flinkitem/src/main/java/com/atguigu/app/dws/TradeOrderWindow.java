package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradeOrderBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: TradeOrderWindow
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/5/17 11:25
 * @Version 1.0
 */
public class TradeOrderWindow {
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
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("dwd_trade_order_detail", "order_detail_221109"), WatermarkStrategy.noWatermarks(), "kafka-source");
        //TODO 3.转换为JSON对象(过滤null值)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (value != null) {
                    out.collect(JSON.parseObject(value));
                }
            }
        });
        //TODO 4.提取时间戳生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("create_time");
            }
        }));
        //TODO 5.按照UID分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS.keyBy(json -> json.getString("user_id"));
        //TODO 6.去重并转换为JavaBean对象
        SingleOutputStreamOperator<TradeOrderBean> tradeOrderDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("order-state", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeOrderBean> out) throws Exception {

                //取出状态数据以及当前数据日期
                String lastDt = valueState.value();
                String curDt = value.getString("create_time").split(" ")[0];

                long ct = 0L;
                long newCt = 0L;

                if (lastDt == null) {
                    ct = 1L;
                    newCt = 1L;
                    valueState.update(curDt);
                } else if (!lastDt.equals(curDt)) {
                    ct = 1L;
                    valueState.update(curDt);
                }

                if (ct == 1L) {
                    out.collect(new TradeOrderBean("", "", ct, null));
                }
            }
        });
        //TODO 7.开窗、聚合
        SingleOutputStreamOperator<TradeOrderBean> resultDS = tradeOrderDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        return value1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {

                        TradeOrderBean next = values.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        out.collect(next);
                    }
                });
        //TODO 8.将数据写出
        resultDS.print(">>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_order_window values(?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("Dws07_TradeOrderWindow");
    }
}
