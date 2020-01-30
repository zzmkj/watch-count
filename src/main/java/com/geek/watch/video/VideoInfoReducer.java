package com.geek.watch.video;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Classname: VideoInfoReducer
 * @Author: Ming
 * @Date: 2020/1/27 2:41 下午
 * @Version: 1.0
 * @Description: TODO
 **/
public class VideoInfoReducer extends Reducer<Text, VideoInfoWritable, Text, VideoInfoWritable> {
    @Override
    protected void reduce(Text key, Iterable<VideoInfoWritable> values, Context context) throws IOException, InterruptedException {
        long goldSum = 0;
        long watchnumpvSum = 0;
        long followerSum = 0;
        long lengthSum = 0;
        for (VideoInfoWritable value : values) {
            goldSum += value.getGold();
            watchnumpvSum += value.getWatchnumpv();
            followerSum += value.getFollower();
            lengthSum += value.getLength();
        }

        VideoInfoWritable result = new VideoInfoWritable(goldSum, watchnumpvSum, followerSum, lengthSum);
        context.write(key, result);
    }
}
