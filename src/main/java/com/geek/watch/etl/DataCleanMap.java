package com.geek.watch.etl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Classname: DataCleanMap
 * @Author: Ming
 * @Date: 2020/1/26 2:17 下午
 * @Version: 1.0
 * @Description: 实现自定义Map类，实现具体的清洗逻辑
 **/
public class DataCleanMap extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取每一行数据
        String line = value.toString();
        //json字符串转成json对象
        JSONObject jsonObject = JSON.parseObject(line);
        //获取需要的字段信息 【注意：在获取数值的时候建议使用getIntValue，如果字段值缺失，则返回0，这样就无须在进行判断】
        if (jsonObject == null) {
            return;
        }
        String uid = jsonObject.getString("uid");
        int gold = jsonObject.getIntValue("gold");
        int watchnumpv = jsonObject.getIntValue("watchnumpv");
        int follower = jsonObject.getIntValue("follower");
        int length = jsonObject.getIntValue("length");

        StringBuilder builder = new StringBuilder();
        //过滤掉异常数据
        if (StringUtils.isNotBlank(uid) && gold >= 0 && watchnumpv >= 0 && follower >= 0 && length >= 0) {
            builder.append(gold).append("\t")
                    .append(watchnumpv).append("\t")
                    .append(follower).append("\t")
                    .append(length);
            context.write(new Text(uid), new Text(builder.toString()));
        }
    }
}
