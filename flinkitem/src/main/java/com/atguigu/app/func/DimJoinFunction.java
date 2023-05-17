package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
     String getKey(T input) throws Exception;

     void join(T input, JSONObject dimInfo) throws Exception;
}
