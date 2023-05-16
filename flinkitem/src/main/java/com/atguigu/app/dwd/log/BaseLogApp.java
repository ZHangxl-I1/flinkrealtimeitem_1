package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * ClassName: BaseLogApp
 * Package: com.atguigu.app.dwd.log
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/16 16:13
 * @Version 1.2
 */
public class BaseLogApp {

    public static void main(String[] args) throws Exception {


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


        //读取kafka topic_log 数据，创建流

        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("topic_log", "base_log_app"), WatermarkStrategy.noWatermarks(), "kafka-source");


        //过滤数据 ,并转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws JSONException {

                if (value != null) {

                    try {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        out.collect(jsonObject);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    }

                }


            }
        });

        /**
         * "common": {							--公共信息
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
         */

        //按mid分组

        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        //对is_new字段处理(状态编程)

        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {


            private ValueState<String> lastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-state", String.class));

            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //获取状态
                String lastDt = lastDtState.value();

                //获取is_new 标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //获取日志数据的ts，当前数据时间
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);


                //判断is_new

                if ("1".equals(isNew)) {

                    if (lastDt == null || lastDt.equals(curDt)) {
                        lastDtState.update(curDt);

                    } else {
                        //以上不满足，则是老用户，则将is_new 改成0
                        value.getJSONObject("common").put("is_new", "0");
                    }


                } else {

                    if (lastDt == null) {
                        //则是老用户,但实时数仓之前没有在状态存储数据，则将状态日期转成1920-01-01
                        lastDtState.update("1970-01-01");

                    }


                }

                return value;
            }


        });

        // 使用侧输出流输出，启动、曝光、动作、错误、播放视频，主流存放页面
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> actionsTag = new OutputTag<String>("actions") {
        };

        OutputTag<String> displaysTag = new OutputTag<String>("displays") {
        };

        OutputTag<String> errTag = new OutputTag<String>("err") {
        };
        OutputTag<String> appVideoTag = new OutputTag<String>("appVideo") {
        };

        /**
         * 启动、页面、曝光、动作、错误、播放：
         * 	启动、页面、播放:互斥
         * 	页面--曝光、动作:包含
         * 	启动、页面、播放--错误:共生
         */

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {


            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {


                //尝试获取错误信息
                String err = value.getString("err");
                if (err != null) {
                    ctx.output(errTag, value.toJSONString());
                    value.remove("err");
                }


                //尝试获取启动数据
                String start = value.getString("start");
                String appVideo = value.getString("appVideo");
                if (start != null) {
                    ctx.output(startTag, value.toJSONString());
                    value.remove("start");
                    //尝试获取播放数据
                } else if (appVideo!=null){
                        ctx.output(appVideoTag, value.toJSONString());
                        value.remove("appVideo");
                } else{
                    /**
                     * "page": {							--页面信息
                     *     "during_time": 11622,			--持续时间毫秒
                     *     "item": "57",					--目标id
                     *     "item_type": "course_id",		--目标类型
                     *     "last_page_id": "course_list",	--上页类型
                     *     "page_id": "course_detail"		--页面id
                     *   }
                     */
                    //
                    Long ts = value.getLong("ts");
                    String pageId = value.getJSONObject("page").getString("page_id");

                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {

                        for (int i = 0; i < displays.size(); i++) {


                            JSONObject jsonObject = displays.getJSONObject(i);
                            //添加曝光中的页面字段
                            jsonObject.put("ts", ts);
                            jsonObject.put("page_id", pageId);

                            ctx.output(displaysTag, jsonObject.toJSONString());


                        }

                    }
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {

                        for (int i = 0; i < actions.size(); i++) {


                            JSONObject jsonObject = actions.getJSONObject(i);
                            //添加曝光中的页面字段
                            jsonObject.put("ts", ts);
                            jsonObject.put("page_id", pageId);

                            ctx.output(displaysTag, jsonObject.toJSONString());


                        }

                    }

                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());

                }
            }
        });

        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> actionsDS = pageDS.getSideOutput(actionsTag);
        DataStream<String> displaysDS = pageDS.getSideOutput(displaysTag);
        DataStream<String> errDS = pageDS.getSideOutput(errTag);
        DataStream<String> appVideoDS = pageDS.getSideOutput(appVideoTag);

        startDS.print("startDS>>>>");
        actionsDS.print("actionsDS>>>>");
        displaysDS.print("displaysDS>>>>");
        errDS.print("errDS>>>>");
        appVideoDS.print("appVideoDS>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";
        String appVideo_topic = "dwd_traffic_appVideo_log";


        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displaysDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionsDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));
        appVideoDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(appVideo_topic));

        env.execute();

    }

}
