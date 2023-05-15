package com.atguigu.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

/**
 * ClassName: MyFlinkCDCUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/15 9:43
 * @Version 1.2
 */
public class MyFlinkCDCUtil {

    public static MySqlSource<String> getMySqlSource(String datsbase,String tableName){

        return MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList(datsbase)
                .tableList(tableName)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();


    }
}
