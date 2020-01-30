package com.geek.watch.top;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Classname: VideoInfoTopMapper
 * @Author: Ming
 * @Date: 2020/1/27 7:51 下午
 * @Version: 1.0
 * @Description: TODO
 **/
public class VideoInfoTopMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String uid = fields[0];
        long length = Long.parseLong(fields[4]);

        context.write(new Text(uid), new LongWritable(length));
    }
}
