package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DruidDSUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;
import java.util.Collection;
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
        return "upsert into "+ GmallConfig.HBASE_SCHEMA+"."+sinkTable+"("+ StringUtils.join(columns,",")+") values ('"+StringUtils.join(values,"','")+"')";


    }
}
