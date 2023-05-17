package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: TradeProvinceOrderWindow
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/5/17 14:52
 * @Version 1.0
 */
public class TradeProvinceOrderWindow {
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
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("dwd_trade_order_detail", "province_order_221109"), WatermarkStrategy.noWatermarks(), "kafka-source");

        //TODO 3.过滤Null值并转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (value != null) {
                    out.collect(JSON.parseObject(value));
                }
            }
        });
        //TODO 4.按照订单明细ID分组,去重由Left Join产生的重复数据，并直接转换为JavaBean对象
        SingleOutputStreamOperator<com.atguigu.bean.TradeProvinceOrderWindow> tradeProvinceDS = jsonObjDS
                .keyBy(json -> json.getString("id"))
                .flatMap(new RichFlatMapFunction<JSONObject, com.atguigu.bean.TradeProvinceOrderWindow>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .build();
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                        stateDescriptor.enableTimeToLive(ttlConfig);

                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<com.atguigu.bean.TradeProvinceOrderWindow> out) throws Exception {

                        //取出状态数据
                        String state = valueState.value();

                        if (state == null) {
                            valueState.update("1");

                            HashSet<String> orderIds = new HashSet<>();
                            orderIds.add(value.getString("order_id"));

                            out.collect(new com.atguigu.bean.TradeProvinceOrderWindow(
                                    "",
                                    "",
                                    value.getString("province_id"),
                                    "",
                                    orderIds,
                                    0L,
                                    0L,
                                    value.getBigDecimal("split_total_amount"),
                                    value.getLong("create_time")));
                        }
                    }
                });
        //TODO 5.提取时间戳生成Watermark
        SingleOutputStreamOperator<com.atguigu.bean.TradeProvinceOrderWindow> tradeProvinceWithWMDS = tradeProvinceDS.assignTimestampsAndWatermarks(WatermarkStrategy.<com.atguigu.bean.TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<com.atguigu.bean.TradeProvinceOrderWindow>() {
            @Override
            public long extractTimestamp(com.atguigu.bean.TradeProvinceOrderWindow element, long recordTimestamp) {
                return element.getTs();
            }
        }));



        //TODO 6.分组、开窗、聚合
        SingleOutputStreamOperator<com.atguigu.bean.TradeProvinceOrderWindow> reduceDS = tradeProvinceWithWMDS.keyBy(com.atguigu.bean.TradeProvinceOrderWindow::getProvinceId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<com.atguigu.bean.TradeProvinceOrderWindow>() {
                    @Override
                    public com.atguigu.bean.TradeProvinceOrderWindow reduce(com.atguigu.bean.TradeProvinceOrderWindow value1, com.atguigu.bean.TradeProvinceOrderWindow value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.setUserCount(value1.getUserCount()+value2.getUserCount());
                        value1.getOrderIds().addAll(value2.getOrderIds());

                        //value1.setOrderCount((long) value1.getOrderIds().size());

                        return value1;
                    }
                }, new WindowFunction<com.atguigu.bean.TradeProvinceOrderWindow, com.atguigu.bean.TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<com.atguigu.bean.TradeProvinceOrderWindow> input, Collector<com.atguigu.bean.TradeProvinceOrderWindow> out) throws Exception {

                        com.atguigu.bean.TradeProvinceOrderWindow next = input.iterator().next();

                        next.setOrderCount((long) next.getOrderIds().size());
                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        out.collect(next);
                    }
                });

        reduceDS.print(">>>>>>");
        //TODO 7.关联维表,补充省份名称
        SingleOutputStreamOperator<com.atguigu.bean.TradeProvinceOrderWindow> tradeProvinceWithDimDS
                = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<com.atguigu.bean.TradeProvinceOrderWindow>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(com.atguigu.bean.TradeProvinceOrderWindow input) throws Exception {
                        return input.getProvinceId();
                    }

                    @Override
                    public void join(com.atguigu.bean.TradeProvinceOrderWindow input, JSONObject dimInfo) throws Exception {
                        input.setProvinceName(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);
        //TODO 8.将数据写出
        tradeProvinceWithDimDS.print(">>>>>>>>>");
        tradeProvinceWithDimDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_province_order_window " +
                "values(?,?,?,?,?,?,?,?)"));
        //TODO 9.启动任务
        env.execute("Dws10_TradeProvinceOrderWindow");
    }
}
