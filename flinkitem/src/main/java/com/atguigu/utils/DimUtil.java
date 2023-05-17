package com.atguigu.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.EDUConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        //查询Redis
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName.toUpperCase() + ":" + id;
        String dimInfoStr = jedis.get(redisKey);
        if (dimInfoStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 3600);
            jedis.close();
            return JSON.parseObject(dimInfoStr);
        }

        //拼接SQL
        String sql = "select * from " + EDUConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";
        System.out.println("查询SQL为：" + sql);

        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection,
                sql,
                JSONObject.class,
                false);

        //将数据写入Redis
        JSONObject dimInfo = jsonObjects.get(0);
        jedis.setex(redisKey, 24 * 3600, dimInfo.toJSONString());
        jedis.close();

        //返回结果
        return jsonObjects.get(0);
    }

    public static void main(String[] args) throws Exception {

        DruidPooledConnection connection = DruidDSUtil.getPhoenixConn();

        long start = System.currentTimeMillis();

        //145 123 115 116 117 120 117
        JSONObject dimBaseTrademark = getDimInfo(connection, "DIM_USER_INFO", "14");
        long end = System.currentTimeMillis();

        //9  9  8  8   1  1  1  1
        JSONObject dimBaseTrademark2 = getDimInfo(connection, "DIM_USER_INFO", "14");
        long end2 = System.currentTimeMillis();

        System.out.println(dimBaseTrademark);
        System.out.println(dimBaseTrademark2);
        System.out.println(end - start);
        System.out.println(end2 - end);

        connection.close();
    }
}
