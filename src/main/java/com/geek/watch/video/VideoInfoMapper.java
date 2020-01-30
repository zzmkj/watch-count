package com.geek.watch.video;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Classname: VideoInfoMapper
 * @Author: Ming
 * @Date: 2020/1/27 2:41 下午
 * @Version: 1.0
 * @Description: TODO
 **/
public class VideoInfoMapper extends Mapper<LongWritable, Text, Text, VideoInfoWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String uid = fields[0];
        long gold = Long.parseLong(fields[1]);
        long watchnumpv = Long.parseLong(fields[2]);
        long follower = Long.parseLong(fields[3]);
        long length = Long.parseLong(fields[4]);

        VideoInfoWritable info = new VideoInfoWritable(gold, watchnumpv, follower, length);
        context.write(new Text(uid), info);
    }
}
