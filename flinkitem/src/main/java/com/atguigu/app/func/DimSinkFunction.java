package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.EDUConfig;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.JedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Map;
import java.util.Set;


/**
 * ClassName: DimSinkFunction
 * Package: com.atguigu.app.func
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/15 14:25
 * @Version 1.2
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {



    //value:{"database":"gmall-221109-flink","table":"comment_info","type":"insert","ts":1669162958,"xid":1111,"xoffset":13941,"data":{"id":1595211185799847960,"user_id":119,"nick_name":null,"head_img":null,"sku_id":31,"spu_id":10,"order_id":987,"appraise":"1204","comment_txt":"评论内容：48384811984748167197482849234338563286217912223261","create_time":"2022-08-02 08:22:38","operate_time":null},需要添加的字段"sink_table":"dim_comment_info"}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {


        //如果是修改操作，在写入Phoenix之前，现将数据写入到redis里
        //将数据按照表名和id进行拼接为唯一字段

        String sinkTable = value.getString("sink_table");
        String id = value.getJSONObject("data").getString("id");

        if("update".equals(value.getString("type"))){

            Jedis jedis = JedisUtil.getJedis();
            String redisKey = "DIM:"+sinkTable.toUpperCase()+":"+id;


            JSONObject jsonObject = new JSONObject();

            Set<Map.Entry<String, Object>> entries = value.getJSONObject("data").entrySet();

            for (Map.Entry<String, Object> entry : entries) {

                Object entryValue = entry.getValue();

                if (entryValue!=null){
                    jsonObject.put(entry.getKey().toUpperCase(),entry.toString());

                }else {
                    jsonObject.put(entry.getKey().toUpperCase(),null);

                }
            }
            jedis.setex(redisKey,24*3600,jsonObject.toJSONString());

            jedis.close();

        }


        DruidPooledConnection phoenixConn = DruidDSUtil.getPhoenixConn();

        //sql语句 upsert into database.table(id,namem,sex) values('1002','zzs','male')

        String upsertSQl = genSql(value.getString("sink_table"),value.getJSONObject("data"));

        System.out.println("upsertSQl>>>"+upsertSQl);


        PreparedStatement preparedStatement = phoenixConn.prepareStatement(upsertSQl);

        preparedStatement.execute();

        //需提交
        phoenixConn.commit();

        //关闭资源
        preparedStatement.close();

        //归还
        phoenixConn.close();

    }

    private String genSql(String sinkTable, JSONObject data) {

        Set<String> columns = data.keySet();

        Collection<Object> values = data.values();

        //sql语句 upsert into database.table(id,namem,sex) values('1002','zzs','male')
        return "upsert into "+ EDUConfig.HBASE_SCHEMA+"."+sinkTable+"("+ StringUtils.join(columns,",")+") values ('"+StringUtils.join(values,"','")+"')";


    }
}
