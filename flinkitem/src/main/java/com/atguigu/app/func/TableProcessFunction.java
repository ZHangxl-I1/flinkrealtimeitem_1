package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * ClassName: TableProcessFunction
 * Package: com.atguigu.app.func
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/15 9:57
 * @Version 1.2
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> stateDescriptor;
    private HashMap<String, TableProcess> tableProcessHashMap;
    private OutputTag<JSONObject> hbaseDS;


    public TableProcessFunction(MapStateDescriptor<String, TableProcess> stateDescriptor, OutputTag<JSONObject> hbaseDS) {
        this.stateDescriptor = stateDescriptor;
        this.hbaseDS = hbaseDS;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tableProcessHashMap = new HashMap<>();

        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/edu_config?" +
                "user=root&password=000000&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");

        String sql = "select * from table_process";
        List<TableProcess> tableProcesses = JdbcUtil.querList(connection, sql, TableProcess.class, true);



        for (TableProcess tableProcess : tableProcesses) {

            String table = tableProcess.getSourceTable();
            if ("dim".equals(tableProcess.getSinkType())) {

                //dim
                //预加载，建表
                checkTable(tableProcess);
                tableProcessHashMap.put(table, tableProcess);
            } else {

                String key = table + "-" + tableProcess.getSourceType();
                tableProcessHashMap.put(key, tableProcess);

            }

        }

        connection.close();


    }


    //广播流
    //value数据格式{"before":null,"after":{"source_table":"base_category3","sink_table":"dim_base_category3","sink_columns":"id,name,category2_id","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1669162876406,"snapshot":"false","db":"gmall-220623-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1669162876406,"transaction":null}

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);

        JSONObject jsonObject = JSONObject.parseObject(value);

        if ("d".equals(jsonObject.getString("op"))) {
            TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("before"), TableProcess.class);

            String sinkType = tableProcess.getSinkType();
            String table = tableProcess.getSourceTable();
            String sourceType = tableProcess.getSourceType();
            if ("dim".equals(sinkType)) {
                broadcastState.remove(table);
                tableProcessHashMap.remove(table);

            } else {

                String key = table + "-" + sourceType;
                broadcastState.remove(key);
                tableProcessHashMap.remove(key);

            }


        } else {
            TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("after"), TableProcess.class);

            String sinkTable = tableProcess.getSinkTable();

            if ("dim".equals(sinkTable)) {

                //建议建表
                checkTable(tableProcess);

                //将数据写入状态
                broadcastState.put(tableProcess.getSourceTable(), tableProcess);

            } else {
                String key = tableProcess.getSourceTable() + "-" + tableProcess.getSourceType();
                broadcastState.put(key, tableProcess);


            }


        }


    }

    private void checkTable(TableProcess tableProcess) {
        PreparedStatement preparedStatement = null;
        DruidPooledConnection phoenixConn = null;


        //表里字段
        String sinkTable = tableProcess.getSinkTable();
        String sinkPk = tableProcess.getSinkPk();
        String sinkColumns = tableProcess.getSinkColumns();
        String sinkExtend = tableProcess.getSinkExtend();


        try {
            //获取连接phoenix
            phoenixConn = DruidDSUtil.getPhoenixConn();

            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            //拼接sql

            //phoenix建表语句create table if not exists db.sinkTable(id varchar primary key,name varchar,sex varchar) xxx

            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");


            String[] columnsArr = sinkColumns.split(",");

            for (int i = 0; i < columnsArr.length; i++) {

                String columns = columnsArr[i];
                if (sinkPk.equals(columns)) {
                    createTableSql.append(columns).append(" varchar primary key");
                } else {
                    createTableSql.append(columns).append(" varchar");
                }

                if (i < columnsArr.length - 1) {
                    createTableSql.append(",");
                } else {
                    createTableSql.append(") ").append(sinkExtend);
                }

            }
            System.out.println("建表createTableSql>>"+createTableSql);

            //预编sql
            preparedStatement = phoenixConn.prepareStatement(createTableSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("建表" + sinkTable + "失败！");
        } finally {
            //归还连接

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            if (phoenixConn != null) {
                try {
                    phoenixConn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

        }

    }


    //主流

    /**
     * {"database":"edu","table":"order_info","type":"update","ts":1645671780,"xid":35594,"commit":true,"data":{"id":34382,"user_id":276,"origin_amount":200.00,"coupon_reduce":0.00,"final_amount":200.00,"order_status":"1002","out_trade_no":"486215529736357","trade_body":"尚硅谷大数据技术之HadoopHA等1件商品","session_id":"9f1fd99e-ca10-450f-a53a-d6af48ba1437","province_id":3,"create_time":"2022-02-21 23:09:51","expire_time":"2022-02-21 23:24:51","update_time":"2022-02-21 23:09:57"},"old":{"order_status":"1001","update_time":null}}
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {


        //获取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);

        String table = value.getString("table");
        String type = value.getString("type");

        //dim
        TableProcess dimTableProcess = broadcastState.get(table);
        TableProcess dimTableProcessMap = tableProcessHashMap.get(table);
        //insert update bootstrap-insert

        //行过滤
        if ((dimTableProcess != null || dimTableProcessMap != null) && ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type))) {

            if (dimTableProcess == null) {
                dimTableProcess = dimTableProcessMap;
            }


            //列过滤
            fliterColumns(value.getJSONObject("data"), dimTableProcess.getSinkColumns());


            //添加sink_table字段
            value.put("sink_table", dimTableProcess.getSinkTable());
            ctx.output(hbaseDS, value);
        } else {
            System.out.println("该表：" + table + "没有对应的配置信息!");

        }


        //dwd
        String key = table + "-" + type;
        TableProcess dwdTableProcess = broadcastState.get(key);
        TableProcess dwdTableProcessMap = tableProcessHashMap.get(key);

        if (dwdTableProcess != null || dwdTableProcessMap != null) {
            if (dwdTableProcess == null) {
                dwdTableProcess = dwdTableProcessMap;

            }

            fliterColumns(value.getJSONObject("data"), dwdTableProcess.getSinkColumns());

            //补充sink_table字段
            value.put("sink_table", dwdTableProcess.getSinkTable());
            out.collect(value);


        } else {
            System.out.println("该表：" + table + "没有对应的配置信息!");

        }


    }

    private void fliterColumns(JSONObject data, String sinkColumns) {

        String[] columns = sinkColumns.split(",");

        List<String> columnsList = (List<String>) Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();

        entries.removeIf(entry -> !columnsList.contains(entry.getKey()));


    }


}



