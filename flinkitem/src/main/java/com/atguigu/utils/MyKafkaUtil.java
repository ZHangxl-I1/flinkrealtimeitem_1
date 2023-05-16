package com.atguigu.utils;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * ClassName: MyKafkaUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/15 9:28
 * @Version 1.2
 */
public class MyKafkaUtil {


    private static  String BOOTSTRAP_SERVER = "hadoop104:9092";

    public static KafkaSource<String> getKafkaSource(String topic,String groupId) {


        return KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        //判断数据是否为空
                        if (message!=null){
                            return new String(message);
                        }else {

                            return null;
                        }
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();


    }

    //单主题写入kafka
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic){
        return new FlinkKafkaProducer<>(BOOTSTRAP_SERVER,topic,new SimpleStringSchema());
    }



    //写入kafka主题
    public static <T>FlinkKafkaProducer<T>  getFlinkKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        return new FlinkKafkaProducer<T>("dwd_default_topic", kafkaSerializationSchema,properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);


    }

    //创建kafka读取topic_db数据后创建的表  普通的kafka连接
    //将消费组id当参数传进来
    public static String getTopicDbDDL(String groupId){

        return "CREATE TABLE topic_db (\n" +
                "  `database` String,\n" +
                "  `table` String,\n" +
                "  `type` String,\n" +
                "  `data` Map<String,String>,\n" +
                "  `old` Map<String,String>,\n" +
                "  `pt` as proctime()\n" +
                ") "+getKafkaSoourceConnOption("topic_db",groupId);

    }
    //kafka的配置信息
    public static String getKafkaSoourceConnOption(String topic,String groupId){

        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+BOOTSTRAP_SERVER+"',\n" +
                "  'properties.group.id' = '"+groupId+"',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";

    }

    //kafka配置信息
    public static String getKafkaSinkConnOption(String topic){

        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+BOOTSTRAP_SERVER+"',\n" +
                "  'format' = 'json'\n" +
                ")";

    }

    //upsert kafka配置信息
    public static String getUpsertKafkaSinkConnOption(String topic){
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+BOOTSTRAP_SERVER+"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }


}
