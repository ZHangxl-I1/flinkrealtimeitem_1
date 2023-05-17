package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.CartAddUuBean;
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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: TradeCartAddUuWindow
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/5/17 10:26
 * @Version 1.0
 */
public class TradeCartAddUuWindow {
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
        //TODO 2.读取Kafka DWD层加购主题数据
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("dwd_trade_cart_add", "cart_add_221109"), WatermarkStrategy.noWatermarks(), "kafka-source");

        //TODO 3.将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //TODO 4.提取时间戳生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("create_time");
            }
        }));
        //TODO 5.按照UID进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS.keyBy(json -> json.getString("user_id"));
        //TODO 6.去重并转换为JavaBean对象
        SingleOutputStreamOperator<CartAddUuBean> cartAddDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("cart-state", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                //取出状态数据以及当前数据日期
                String lastDt = valueState.value();
                String curDt = value.getString("create_time").split(" ")[0];

                if (lastDt == null || !lastDt.equals(curDt)) {
                    valueState.update(curDt);
                    out.collect(new CartAddUuBean("", "", 1L, null));
                }

            }
        });

        SingleOutputStreamOperator<CartAddUuBean> resultDS = cartAddDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                return value1;
            }
        }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                CartAddUuBean next = values.iterator().next();

                next.setTs(System.currentTimeMillis());
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                out.collect(next);
            }
        });
        //TODO 8.将数据写出
        resultDS.print(">>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));
        //TODO 9.启动任务
        env.execute("dws_TradeOrderWindow");
    }
}
