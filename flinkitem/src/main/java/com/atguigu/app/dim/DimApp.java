package com.atguigu.app.dim;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyFlinkCDCUtil;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * ClassName: DimApp
 * Package: com.atguigu.app.dim
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/15 9:27
 * @Version 1.2
 */
public class DimApp {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //设置检查点
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


        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("topic_db", "dim_app"), WatermarkStrategy.noWatermarks(), "kafka-source");

//        kafkaDS.print("kafkaDS>>>>");


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


        MySqlSource<String> mySqlSource = MyFlinkCDCUtil.getMySqlSource("edu_config", "edu_config.table_process");


        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");

//        mysqlDS.print("mysqlDS>>>>");

        MapStateDescriptor<String, TableProcess> stateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(stateDescriptor);


        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);


        //侧输出流存放hbase数据 主流写入kafka
        OutputTag<JSONObject> hbaseDS = new OutputTag<JSONObject>("hbase") {
        };

        SingleOutputStreamOperator<JSONObject> processDS = connectedStream.process(new TableProcessFunction(stateDescriptor,hbaseDS));


        //dwd写入kafka
        processDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sink_table"),element.getString("data").getBytes());
            }
        }));


        //dim写入hbase
      processDS.getSideOutput(hbaseDS).addSink(new DimSinkFunction());

      env.execute();



    }
}
