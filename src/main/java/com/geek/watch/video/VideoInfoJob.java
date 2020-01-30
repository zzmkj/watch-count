package com.geek.watch.video;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Classname: VideoInfoJob
 * @Author: Ming
 * @Date: 2020/1/27 2:40 下午
 * @Version: 1.0
 * @Description: 统计直播四个维度：
 *                      金币数量(gold)、总观看PV(watchnumpv)、粉丝关注量(follower)、视频总开播时长(length)
 **/
public class VideoInfoJob {
    public static void main(String[] args) throws Exception {
        /*if (args.length != 2) {
            System.exit(100);
        }*/
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
//        Path outputPath = new Path("video/info");
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(VideoInfoJob.class);

        job.setMapperClass(VideoInfoMapper.class);
        job.setReducerClass(VideoInfoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VideoInfoWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VideoInfoWritable.class);

//        FileInputFormat.setInputPaths(job, new Path("video/etl"));
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.out.println("result : " + result);
    }
}
