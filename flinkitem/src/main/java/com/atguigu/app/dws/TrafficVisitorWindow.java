package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradeOrderBean;
import com.atguigu.bean.TrafficSourceBean;
import com.atguigu.bean.TrafficVisitorBean;
import com.atguigu.common.EDUConfig;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: TrafficVisitorWindow
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/19 13:56
 * @Version 1.2
 */
public class TrafficVisitorWindow {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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


        //读取kakfa dwd层页面主题数据
        /**
         *  "common": {							--公共信息
         *     "ar": "16",							--地区编码
         *     "ba": "iPhone",						--手机品牌
         *     "ch": "Appstore",					--渠道
         *     "is_new": "1",						--是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0。
         *     "md": "iPhone 8",					--手机型号
         *     "mid": "mid_161",					--设备id
         *     "os": "iOS 13.3.1",					--操作系统
         *     "sc": "2",							--来源
         *     "sid": "9acef85b-067d-49f9-9520-a0dda943304e",	--会话id
         *     "uid": "272",						--会员id
         *     "vc": "v2.1.134"					--APP版本号
         *   },
         *   "page": {							--页面信息
         *     "during_time": 11622,			--持续时间毫秒
         *     "item": "57",					--目标id
         *     "item_type": "course_id",		--目标类型
         *     "last_page_id": "course_list",	--上页类型
         *     "page_id": "course_detail"		--页面id
         *   },
         */
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource(EDUConfig.PAGE_TOPIC, "traffic_source"), WatermarkStrategy.noWatermarks(), "kafka-source");


        //数据转换成json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //清洗is_new字
//        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("user_id"));
//
//        SingleOutputStreamOperator<TrafficVisitorBean> tradeOrderDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficVisitorBean>() {
//
//            private ValueState<String> valueState;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitor-state", String.class));
//            }
//
//            @Override
//            public void flatMap(JSONObject value, Collector<TrafficVisitorBean> out) throws Exception {
//
//                //取出状态数据以及当前数据日期
//                String lastDt = valueState.value();
//                String curDt = value.getString("ts");
//
//                long ct = 0L;
//                long newCt = 0L;
//
//                if (lastDt == null) {
//                    ct = 1L;
//                    newCt = 1L;
//                    valueState.update(curDt);
//                } else if (!lastDt.equals(curDt)) {
//                    ct = 1L;
//                    valueState.update(curDt);
//                }
//
//                if (ct == 1L) {
//                    out.collect());
//                }
//            }
//        });





//        //按照mid 分组
//        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
//
//
//        SingleOutputStreamOperator<TrafficSourceBean> trafficSourceMidDS = keyedByMidDS.map(new RichMapFunction<JSONObject, TrafficSourceBean>() {
//
//
//            private ValueState<String> valueState;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//
//                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
//                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
//                        .build();
//
//                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("mid-state", String.class);
//
//                stateDescriptor.enableTimeToLive(ttlConfig);
//                valueState = getRuntimeContext().getState(stateDescriptor);
//
//            }
//
//            @Override
//            public TrafficSourceBean map(JSONObject value) throws Exception {
//
//                String lastDt = valueState.value();
//                long uvCt = 0L;
//                if (lastDt == null) {
//                    uvCt = 1L;
//                    valueState.update("1");
//                }
//
//                return TrafficSourceBean.builder()
//                        .page(value.getJSONObject("page"))
//                        .pageId(value.getJSONObject("page").getString("page_id"))
//                        .sid(value.getJSONObject("common").getString("sid"))
//                        .sc(value.getJSONObject("common").getString("sc"))
//                        .uvCt(uvCt)
//                        .ts(value.getLong("ts"))
//                        .durSum(value.getJSONObject("page").getLong("during_time"))
//                        .build();
//            }
//        });
//



    }
}
